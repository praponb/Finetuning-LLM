from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_sessions
    .withColumn("new_group_flag", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessions_sessions = with_gaps.withColumn("island_id", F.sum("new_group_flag").over(w2))

result = (
    sessions_sessions
    .groupBy(
        F.when(F.col("new_group_flag") >= 1, "PREMIUM").otherwise("STANDARD").alias("new_group_flag_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.max("started_at").alias("last_seen"),
        F.count_if(F.col("country") == "TH").alias("th_events"),
    )
    .filter(F.col("row_count") > 1)
    .orderBy(F.col("row_count").desc_nulls_last())
)
