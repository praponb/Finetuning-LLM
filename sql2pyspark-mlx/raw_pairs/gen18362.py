from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_prev = df_sessions.withColumn("prev_started_at", F.lag("started_at").over(w))

with_gaps = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_sessions = with_gaps.withColumn("session_id", F.sum("is_new_session").over(w2))

result = (
    sessions_sessions
    .groupBy(F.date_trunc("WEEK", "prev_started_at").alias("week_start"))
    .agg(
        F.round(F.sum("session_id") / F.nullif(F.count(F.when(F.col("browser") == "Edge", 1)), F.lit(0)), 2).alias("session_id_per_edge"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.countDistinct("country").alias("distinct_countries"),
        F.countDistinct("browser").alias("distinct_browsers"),
    )
)
