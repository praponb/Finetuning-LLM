from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_sessions
    .withColumn("is_new_session", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_id", F.sum("is_new_session").over(w2))

result = (
    sessionized
    .withColumn("is_new_session_tier", F.when(F.col("is_new_session") >= 1, "LARGE").otherwise("SMALL"))
    .withColumn("activity_month", F.date_trunc("MONTH", "started_at"))
    .groupingSets(
        [["is_new_session_tier", "activity_month"], ["is_new_session_tier"], []],
        "is_new_session_tier",
        "activity_month",
    )
    .agg(
        F.size(F.collect_set("country")).alias("num_countries"),
        F.min("session_id").alias("min_session_id"),
    )
)
