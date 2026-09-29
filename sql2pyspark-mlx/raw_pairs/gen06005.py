from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_transactions
    .filter(~F.col("merchant").eqNullSafe(F.lit("7-Eleven")))
    .select(
        "*",
        F.upper("txn_type").alias("txn_type_upper"),
        F.coalesce("merchant", F.lit("DIRECT")).alias("merchant_filled"),
    )
)

with_gaps = (
    base
    .withColumn("new_group_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_no", F.sum("new_group_flag").over(w2))

result = (
    islands
    .withColumn("activity_month", F.date_trunc("month", "txn_time"))
    .groupingSets(
        [["activity_month", "merchant"], ["activity_month"]],
        "activity_month",
        "merchant",
    )
    .agg(
        F.percentile_approx("amount", 0.9).alias("amount_p90"),
        F.count(F.when(F.col("txn_type") == "REFUND", 1)).alias("refund_count"),
        F.grouping("activity_month").alias("grouping_level"),
    )
)
