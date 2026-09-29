from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_sessions
    .withColumn("new_group_flag", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessionized = (
    with_gaps
    .select(
        "id",
        "customer_id",
        "started_at",
        "country",
        F.sum("new_group_flag").over(w2).alias("session_id"),
    )
)

result = (
    sessionized
    .groupBy(
        F.when(F.col("session_id") >= 7, "PREMIUM").otherwise("STANDARD").alias("session_id_tier"),
        "country",
    )
    .agg(
        F.max("started_at").alias("ended_at"),
        F.count_if(F.col("country") == "JP").alias("jp_events"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.percentile_approx("session_id", [0.25, 0.5, 0.75]).alias("session_id_quartiles"),
    )
    .filter(F.col("jp_events") >= 1)
)
