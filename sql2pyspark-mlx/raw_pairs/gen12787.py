from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_sessions
    .filter(F.col("country").isNotNull())
    .select(
        "id",
        "customer_id",
        "started_at",
        F.when(F.col("device").isin(["tablet", "desktop"]), "Tier 1").otherwise("Other").alias("device_group"),
    )
)

with_prev = base.withColumn("prev_started_at", F.lag("started_at").over(w))

gaps = (
    with_prev
    .withColumn("new_group_flag", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_id", F.sum("new_group_flag").over(w2))

result = (
    sessionized
    .groupBy("customer_id")
    .agg(
        F.percentile_approx("new_group_flag", 0.5).alias("new_group_flag_p50"),
        F.min("session_id").alias("min_session_id"),
        F.min("session_id").alias("min_session_id_2"),
        F.count("*").alias("cnt"),
    )
)
