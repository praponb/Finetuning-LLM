from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_sessions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        F.substring("device", 1, 4).alias("device_prefix"),
    )
)

with_prev = cleaned.withColumn("prev_started_at", F.lag("started_at").over(w))

flagged = (
    with_prev
    .withColumn("is_new_burst", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_burst").over(w2))

result = (
    sessionized
    .groupBy(
        F.when(F.col("session_no") >= 7, "LARGE").otherwise("SMALL").alias("session_no_tier"),
    )
    .agg(
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.countDistinct("device").alias("distinct_devices"),
    )
    .orderBy(F.col("distinct_devices").desc_nulls_first())
)
