from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("sale_date").desc_nulls_first(), "id")
w2 = Window.orderBy(F.col("units").desc(), "id")
w3 = Window.partitionBy("store_id").orderBy("sale_date", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("store_id").orderBy(F.col("revenue_avg_3").desc(), "id")
w5 = Window.partitionBy("product_id").orderBy("units_bucket", "id")
w6 = Window.orderBy("units_bucket", "id")
w7 = Window.partitionBy("product_id").orderBy("row_seq", "id")
w8 = Window.partitionBy("product_id").orderBy("units_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
quantity_by_product_id = df_inventory.groupBy("product_id").agg(F.avg("quantity").alias("avg_inventory_quantity"))

base = (
    df_store_sales
    .filter((F.col("sale_date") > F.lit("2023-03-02").cast("date")) & (F.col("units") <= 23))
    .withColumn("greatest_revenue_units", F.greatest("revenue", "units"))
)

deduped = (
    base
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

enriched = (
    deduped
    .withColumn("units_bucket", F.ntile(10).over(w2))
    .withColumn("revenue_avg_3", F.avg("revenue").over(w3))
)

latest = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "product_id",
        "sale_date",
        "units",
        "rank_in_group",
        "units_bucket",
        "revenue_avg_3",
    )
)

with_dims = (
    latest
    .alias("src")
    .join(df_products.alias("d"), F.col("src.product_id") == F.col("d.id"), "left")
    .filter(F.col("d.stock_qty") < 150)
    .select(
        "src.id",
        "src.product_id",
        "src.sale_date",
        "src.units_bucket",
        F.col("d.product_name"),
        F.col("d.stock_qty"),
    )
)

scored = (
    with_dims
    .select(
        "id",
        "product_id",
        "units_bucket",
        "stock_qty",
        F.lag("units_bucket").over(w5).alias("prev_units_bucket"),
        F.lead("units_bucket").over(w5).alias("next_units_bucket"),
        F.lead("stock_qty").over(w5).alias("next_stock_qty"),
        F.lag("stock_qty").over(w5).alias("prev_stock_qty"),
        F.row_number().over(w5).alias("row_seq"),
        F.lag("units_bucket", 1).over(w6).alias("prev_units_bucket_2"),
    )
)

with_windows = (
    scored
    .select(
        "id",
        "product_id",
        "units_bucket",
        "next_units_bucket",
        "next_stock_qty",
        "prev_stock_qty",
        "row_seq",
        F.lag("prev_units_bucket_2", 1).over(w7).alias("prev_prev_units_bucket_2"),
        F.sum("prev_stock_qty").over(w8).alias("cumulative_prev_stock_qty"),
    )
)

result = (
    with_windows
    .join(quantity_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "product_id",
        "units_bucket",
        "next_stock_qty",
        "row_seq",
        "cumulative_prev_stock_qty",
        F.col("avg_inventory_quantity"),
    )
    .orderBy(F.col("units_bucket").desc(), "id")
    .limit(10)
)
