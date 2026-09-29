from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("new_group_flag_tier").orderBy(F.col("distinct_countries").desc())

gaps = (
    df_sessions
    .withColumn("new_group_flag", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_no", F.sum("new_group_flag").over(w2))

result = (
    sessionized
    .groupBy(
        F.when(F.col("new_group_flag") >= 1, "LARGE").otherwise("SMALL").alias("new_group_flag_tier"),
        "session_no",
    )
    .agg(
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.countDistinct("country").alias("distinct_countries"),
    )
    .filter(F.col("distinct_countries") < 1)
    .withColumn("distinct_countries_dense_rank", F.dense_rank().over(w3))
)
