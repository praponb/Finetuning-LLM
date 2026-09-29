from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_sessions
    .withColumn("is_new_session", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_no", F.sum("is_new_session").over(w2))

result = (
    sessionized
    .withColumn("is_new_session_tier", F.when(F.col("is_new_session") >= 1, "PREMIUM").otherwise("STANDARD"))
    .withColumn("week_start", F.date_trunc("WEEK", "started_at"))
    .groupingSets(
        [["is_new_session_tier", "week_start"], ["is_new_session_tier"]],
        "is_new_session_tier",
        "week_start",
    )
    .agg(
        F.size(F.collect_set("country")).alias("num_countries"),
        F.sort_array(F.collect_set("browser")).alias("browser_list"),
        F.round(F.avg("session_no"), 2).alias("avg_session_no"),
        F.grouping_id().alias("grouping_level"),
    )
)
