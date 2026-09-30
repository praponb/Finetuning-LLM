from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("amount_filled_2_adjusted", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy("amount_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy(F.col("cumulative_amount_filled_2_per_failed").desc())

valid = (
    df_payments
    .filter(F.col("paid_at") >= F.lit("2023-04-26 19:00:00").cast("timestamp"))
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "status",
        F.coalesce("amount", F.lit(0)).alias("amount_filled"),
    )
)

base = (
    valid
    .filter(F.col("paid_at") > F.to_date(F.lit("2023-05-11")))
    .select(
        "id",
        "paid_at",
        "status",
        "amount_filled",
        F.coalesce("amount", F.lit(0)).alias("amount_filled_2"),
    )
)

valid_payments = (
    base
    .filter(F.col("amount_filled_2") <= 64)
    .withColumn("amount_filled_2_adjusted", F.round((F.col("amount_filled_2") * 0.85), 2))
)

with_windows = (
    valid_payments
    .select(
        "*",
        F.sum("amount_filled_2").over(w).alias("cumulative_amount_filled_2"),
        F.last("amount_filled_2", ignorenulls=True).over(w2).alias("last_known_amount_filled_2"),
    )
)

totals = (
    with_windows
    .groupBy(
        "status",
        F.date_trunc("QUARTER", "paid_at").alias("quarter_start"),
        F.when(F.col("amount_filled_2") >= 78, "HIGH").otherwise("LOW").alias("amount_filled_2_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.round(F.avg("cumulative_amount_filled_2"), 2).alias("avg_cumulative_amount_filled_2"),
        F.count(F.when(F.col("status") == "SUCCESS", 1)).alias("success_count"),
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("duration_min"),
        F.round(F.sum("cumulative_amount_filled_2") / F.nullif(F.count(F.when(F.col("status") == "FAILED", 1)), F.lit(0)), 2).alias("cumulative_amount_filled_2_per_failed"),
    )
    .withColumn("cumulative_amount_filled_2_per_failed_rank", F.rank().over(w3))
)

result = (
    totals
    .select(
        "status",
        "quarter_start",
        "amount_filled_2_tier",
        "status_list",
        "avg_cumulative_amount_filled_2",
        "success_count",
        "duration_min",
        "cumulative_amount_filled_2_per_failed",
    )
)
