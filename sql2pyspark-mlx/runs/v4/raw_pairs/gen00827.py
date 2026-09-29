from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("country").orderBy(F.col("price").desc(), "id")
w4 = Window.partitionBy("category").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("country").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("supplier_id")
w7 = Window.partitionBy("category").orderBy("cost", "id").rowsBetween(-5, Window.currentRow)
w8 = Window.orderBy("price", "id").rowsBetween(-6, Window.currentRow)
price_by_supplier_id = df_products.groupBy("supplier_id").agg(F.sum("price").alias("sum_product_price"))

cleaned = (
    df_products
    .filter(F.col("launch_date") >= "2024-03-01")
    .select("*", F.coalesce("cost", F.lit(0)).alias("cost_filled"))
)

enriched = (
    cleaned
    .alias("src")
    .join(df_suppliers.alias("d"), F.col("src.supplier_id") == F.col("d.id"), "inner")
    .select("src.*", F.col("d.rating"), F.col("d.supplier_name"), F.col("d.country"))
)

latest_match = (
    enriched
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.unit_price"),
        F.col("c.quantity").alias("order_item_quantity"),
    )
)

with_last = (
    latest_match
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.revenue"))
)

latest = (
    with_last
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

scored = (
    latest
    .select(
        "*",
        F.last("country", ignorenulls=True).over(w4).alias("last_known_country"),
        F.sum("rating").over(w5).alias("running_rating"),
        F.round((F.col("revenue") / F.sum("revenue").over(w6)), 4).alias("revenue_share"),
        F.avg("rating").over(w7).alias("rating_avg_5"),
        F.avg("revenue").over(w8).alias("revenue_avg_6"),
    )
)

result = (
    scored
    .join(price_by_supplier_id, on="supplier_id", how="left")
    .select(
        "id",
        "category",
        "price",
        "launch_date",
        "supplier_id",
        "supplier_name",
        "country",
        "order_item_quantity",
        "revenue",
        F.col("sum_product_price"),
    )
    .orderBy(F.col("supplier_id").desc_nulls_last())
)
