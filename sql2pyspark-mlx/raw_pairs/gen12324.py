from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("sale_date_month").desc_nulls_last(), "id")
w2 = Window.partitionBy("store_id").orderBy("launch_date", "id")
quantity_by_product_id = df_inventory.groupBy("product_id").agg(F.max("quantity").alias("max_inventory_quantity"))

base = (
    df_store_sales
    .filter(F.col("sale_date") >= F.lit("2023-09-11").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.month("sale_date").alias("sale_date_month"),
        F.ceil("revenue").alias("revenue_whole"),
    )
)

deduped = (
    base
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

enriched = (
    deduped
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .select(
        "s.id",
        "s.store_id",
        "s.product_id",
        "s.units",
        F.col("p.launch_date"),
        F.col("p.stock_qty"),
    )
)

with_dims = (
    enriched
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.supplier_id").isin([32, 37]))
    .select(
        "s.id",
        "s.store_id",
        "s.product_id",
        "s.launch_date",
        "s.stock_qty",
        F.col("p.brand"),
        F.col("p.stock_qty").alias("product_stock_qty"),
        F.col("p.cost"),
    )
)

enriched_store_sales = (
    with_dims
    .withColumn("next_product_stock_qty", F.lead("product_stock_qty").over(w2))
    .withColumn("prev_product_stock_qty", F.lag("product_stock_qty").over(w2))
    .withColumn("prev_stock_qty", F.lag("stock_qty").over(w2))
    .withColumn("event_seq", F.row_number().over(w2))
)

result = (
    enriched_store_sales
    .join(quantity_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "product_id",
        "stock_qty",
        "brand",
        "cost",
        "prev_product_stock_qty",
        "prev_stock_qty",
        "event_seq",
        F.when(F.col("brand").rlike("^(Globex|Stark)$"), "PRIORITY").when(F.col("brand").rlike("^(Acme|Initech)$"), "GROUP_A").otherwise("EXTENDED").alias("brand_zone"),
        F.col("max_inventory_quantity"),
    )
)
