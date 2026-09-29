from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method", "status").orderBy(F.col("paid_at").asc_nulls_first(), "id")
w2 = Window.orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("method").orderBy(F.col("amount").desc(), "id")
w4 = Window.partitionBy("status").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.orderBy("amount", "id").rowsBetween(-3, Window.currentRow)
w6 = Window.partitionBy("order_id").orderBy("amount_avg_3", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("order_id").orderBy("cumulative_amount_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("order_id").orderBy(F.col("amount_bucket_p90").desc())

valid = (
    df_payments
    .filter(~F.col("method").eqNullSafe(F.lit("bank_transfer")))
    .select(
        "*",
        F.when(F.col("status").isin(["REFUNDED", "FAILED"]), "Priority").otherwise("Other").alias("status_group"),
        F.when(F.col("status").isin(["SUCCESS", "FAILED"]), "Priority").otherwise("Other").alias("status_group_2"),
    )
)

latest = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

scored = (
    latest
    .filter(F.to_date("paid_at") < "2024-06-01")
    .select(
        "id",
        "order_id",
        F.sum("amount").over(w2).alias("cumulative_amount"),
        F.ntile(10).over(w3).alias("amount_bucket"),
        F.sum("amount").over(w4).alias("cumulative_amount_2"),
        F.avg("amount").over(w_grp).alias("amount_avg_3"),
    )
)

enriched = (
    scored
    .select(
        "id",
        "order_id",
        "cumulative_amount",
        "amount_bucket",
        "amount_avg_3",
        F.count("amount_bucket").over(w6).alias("running_count_amount_bucket"),
        F.max("amount_bucket").over(w6).alias("running_max_amount_bucket"),
        F.sum("amount_bucket").over(w6).alias("running_sum_amount_bucket"),
        F.min("amount_bucket").over(w6).alias("running_min_amount_bucket"),
        F.first("cumulative_amount", ignorenulls=True).over(w7).alias("first_known_cumulative_amount"),
    )
)

result = (
    enriched
    .groupBy(
        "order_id",
        F.when(F.col("running_max_amount_bucket") >= 9.0, "PREMIUM").otherwise("STANDARD").alias("running_max_amount_bucket_tier"),
    )
    .agg(
        F.round(F.avg("amount_bucket"), 2).alias("avg_amount_bucket"),
        F.count("*").alias("num_rows"),
        F.round(F.avg("amount_avg_3"), 2).alias("avg_amount_avg_3"),
        F.percentile_approx("amount_bucket", 0.5).alias("amount_bucket_p50"),
        F.percentile_approx("amount_bucket", 0.9).alias("amount_bucket_p90"),
    )
    .filter(F.col("num_rows") > 1)
    .withColumn("amount_bucket_p90_percent_rank", F.percent_rank().over(w8))
    .orderBy(F.col("running_max_amount_bucket_tier").desc())
)
