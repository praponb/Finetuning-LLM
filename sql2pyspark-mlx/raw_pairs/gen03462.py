from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id")
w2 = Window.partitionBy("budget_tier").orderBy(F.col("avg_budget").desc())

ranked = (
    df_departments
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "location", "budget")
)

result = (
    ranked
    .groupBy(
        F.when(F.col("budget") >= 4445000, "HIGH").otherwise("LOW").alias("budget_tier"),
        "location",
    )
    .agg(
        F.sort_array(F.collect_set("location")).alias("location_list"),
        F.sort_array(F.collect_set("location")).alias("location_list_2"),
        F.count_if(F.col("location") == "New York").alias("new_york_events"),
        F.percentile_approx("budget", 0.95).alias("budget_p95"),
        F.round(F.avg("budget"), 2).alias("avg_budget"),
    )
    .filter(F.col("new_york_events") < 1000)
    .withColumn("avg_budget_dense_rank", F.dense_rank().over(w2))
    .orderBy(F.col("avg_budget").asc_nulls_last())
)
