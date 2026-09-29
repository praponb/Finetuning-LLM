from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_prev = df_sessions.withColumn("prev_started_at", F.lag("started_at").over(w))

flagged = (
    with_prev
    .withColumn("new_group_flag", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessions_sessions = flagged.withColumn("session_no", F.sum("new_group_flag").over(w2))

result = (
    sessions_sessions
    .groupBy(
        "country",
        F.date_trunc("MONTH", "prev_started_at").alias("activity_month"),
    )
    .agg(
        F.min("prev_started_at").alias("first_seen"),
        F.percentile_approx("new_group_flag", 0.9).alias("new_group_flag_p90"),
        ((F.unix_timestamp(F.max("prev_started_at")) - F.unix_timestamp(F.min("prev_started_at"))) / 60).alias("span_minutes"),
    )
    .filter(F.col("span_minutes") > 0)
    .orderBy(F.col("country").desc_nulls_last())
)
