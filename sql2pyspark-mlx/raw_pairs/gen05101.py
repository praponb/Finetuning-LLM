from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy("launch_date", "id").rowsBetween(-5, -1)
w_ordered = Window.orderBy(F.col("launch_date_month_end").desc(), "id")
w3 = Window.partitionBy("category").orderBy("least_price_cost", "id")
w4 = Window.partitionBy("supplier_id").orderBy("price", "id")
w5 = Window.partitionBy("category").orderBy(F.col("price_bucket").desc(), "id")
w6 = Window.partitionBy("category").orderBy(F.col("launch_date_month_end").desc())
stock_qty_by_supplier_id = df_products.groupBy("supplier_id").agg(F.sum("stock_qty").alias("sum_product_stock_qty"))

filtered = (
    df_products
    .filter(F.col("launch_date") > F.lit("2023-08-16").cast("date"))
    .select(
        "id",
        "product_name",
        "category",
        "price",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.least("price", "cost").alias("least_price_cost"),
        F.last_day("launch_date").alias("launch_date_month_end"),
    )
)

with_windows = (
    filtered
    .withColumn("least_price_cost_avg_5_prior", F.avg("least_price_cost").over(w))
    .withColumn("event_seq", F.row_number().over(w_ordered))
    .withColumn("prev_price", F.lag("price", 1).over(w3))
    .withColumn("price_bucket", F.ntile(4).over(w4))
)

latest = (
    with_windows
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
)

deduped = (
    latest
    .withColumn("dedup_rank_2", F.rank().over(w6))
    .filter(F.col("dedup_rank_2") == 1)
)

result = (
    deduped
    .join(stock_qty_by_supplier_id, on="supplier_id", how="left")
    .select(
        "id",
        "category",
        "stock_qty",
        "launch_date",
        "supplier_id",
        "launch_date_month_end",
        "least_price_cost_avg_5_prior",
        "event_seq",
        "price_bucket",
        F.col("sum_product_stock_qty"),
    )
    .orderBy(F.col("id").desc_nulls_last())
    .limit(20)
)
