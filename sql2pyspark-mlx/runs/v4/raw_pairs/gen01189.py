from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("revenue", "id")
w2 = Window.orderBy(F.col("units").desc_nulls_last())
w3 = Window.orderBy(F.col("sale_date_period").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w4 = Window.orderBy("row_seq", "id")
w5 = Window.orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("units_max_30d", "id")
w7 = Window.orderBy("sale_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("row_seq_bucket", "id").rowsBetween(-3, -1)
w9 = Window.orderBy("next_units_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.orderBy("revenue")
w11 = Window.orderBy(F.col("cumulative_units_rank").desc(), "id")

cleaned = (
    df_store_sales
    .filter(F.col("store_id").isin([3, 5]))
    .select(
        "id",
        "units",
        "revenue",
        F.date_trunc("month", "sale_date").alias("sale_date_period"),
    )
)

enriched = (
    cleaned
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("units_rank", F.rank().over(w2))
    .withColumn("units_max_30d", F.max("units").over(w3))
)

enriched_store_sales = (
    enriched
    .withColumn("next_units_rank", F.lead("units_rank", 2).over(w4))
    .withColumn("last_known_units", F.last("units", ignorenulls=True).over(w5))
    .withColumn("units_max_30d_bucket", F.ntile(4).over(w6))
    .withColumn("row_seq_bucket", F.ntile(5).over(w4))
)

with_windows = (
    enriched_store_sales
    .select(
        "*",
        F.sum("units_rank").over(w7).alias("cumulative_units_rank"),
        F.first("last_known_units", ignorenulls=True).over(w7).alias("first_known_last_known_units"),
        F.avg("units").over(w8).alias("units_avg_3_prior"),
        F.sum("last_known_units").over(w9).alias("cumulative_last_known_units"),
    )
)

metrics = (
    with_windows
    .select(
        "*",
        F.percent_rank().over(w10).alias("revenue_percent_rank"),
        F.ntile(10).over(w11).alias("cumulative_units_rank_bucket"),
    )
)

result = (
    metrics
    .select("id", "revenue_percent_rank", F.col("revenue").alias("REVENUE"))
    .unpivot(["id"], ["revenue_percent_rank", "REVENUE"], "measure", "metric_value")
)
