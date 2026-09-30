from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy(F.col("paid_at").asc_nulls_first(), "id")
w2 = Window.partitionBy("status").orderBy("paid_at_day")

base = (
    df_payments
    .filter((F.col("paid_at") > F.lit("2023-06-24 14:00:00").cast("timestamp")) & (~F.col("method").eqNullSafe(F.lit("cash"))))
    .select(
        "*",
        F.round("amount", 2).alias("amount_rounded"),
        F.coalesce("method", F.lit("OTHER")).alias("method_filled"),
    )
)

deduped = (
    base
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "paid_at", "status")
)

valid = (
    deduped
    .filter(F.col("paid_at") >= F.to_timestamp(F.lit("2023-08-09 05:00:00")))
    .select(
        "*",
        F.col("paid_at").cast("date").alias("paid_at_day"),
        F.when(F.col("status").isin(["FAILED", "REFUNDED"]), "Core").otherwise("Other").alias("status_group"),
    )
)

latest = (
    valid
    .withColumn("rn", F.rank().over(w2))
    .filter(F.col("rn") == 1)
    .select("id", "status", "status_group")
)

filtered = (
    latest
    .filter(F.col("status") == "FAILED")
    .select("*", F.upper("status").alias("status_upper"))
)

grouped = (
    filtered
    .groupBy("status")
    .agg(
        F.count(F.when(F.col("status") == "REFUNDED", 1)).alias("refunded_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.count(F.when(F.col("status") == "REFUNDED", 1)).alias("refunded_count_2"),
        F.countDistinct("status").alias("distinct_status"),
    )
    .filter(F.col("distinct_ids") > 1)
)

result = grouped.select("status", "refunded_count", "distinct_ids", "refunded_count_2")
