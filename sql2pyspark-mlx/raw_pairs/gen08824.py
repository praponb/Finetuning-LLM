from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("week_start").orderBy(F.col("uk_count").desc())

flagged = (
    df_sessions
    .withColumn("new_group_flag", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("new_group_flag").over(w2))

result = (
    sessionized
    .groupBy(F.date_trunc("WEEK", "started_at").alias("week_start"), "device")
    .agg(
        F.count(F.when(F.col("country") == "UK", 1)).alias("uk_count"),
        F.sort_array(F.collect_set("browser")).alias("browser_list"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("span_minutes"),
        F.size(F.collect_set("device")).alias("num_devices"),
    )
    .filter(F.col("num_devices") < 1000)
    .withColumn("uk_count_percent_rank", F.percent_rank().over(w3))
    .orderBy(F.col("week_start").desc())
)
