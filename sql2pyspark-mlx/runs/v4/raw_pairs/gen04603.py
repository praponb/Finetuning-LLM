from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("amount", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("order_id").orderBy(F.col("amount").desc(), "id")

with_windows = (
    df_payments
    .select(
        "*",
        F.lag("amount", 2).over(w).alias("prev_amount"),
        F.sum("amount").over(w2).alias("cumulative_amount"),
        F.ntile(4).over(w3).alias("amount_bucket"),
    )
)

result = (
    with_windows
    .withColumn("quarter_start", F.date_trunc("QUARTER", "paid_at"))
    .rollup("quarter_start")
    .agg(
        F.countDistinct("method").alias("distinct_methods"),
        F.stddev_samp("amount").alias("amount_stddev"),
        F.avg("amount_bucket").alias("avg_amount_bucket"),
        F.sum(F.when(F.col("method") == "cash", F.col("amount_bucket")).otherwise(0)).alias("cash_amount_bucket"),
        F.avg("prev_amount").alias("avg_prev_amount"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("amount_stddev").asc_nulls_first())
)
