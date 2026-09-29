from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("location").orderBy(F.col("budget").asc_nulls_last())
w2 = Window.partitionBy("dept_name", "location")

metrics = (
    df_departments
    .select(
        "id",
        "budget",
        F.percent_rank().over(w_running).alias("budget_percent_rank"),
        F.round((F.col("budget") / F.sum("budget").over(w2)), 4).alias("budget_share"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("budget") >= 4398000, "HIGH").otherwise("LOW").alias("budget_tier"),
        F.when(F.col("budget_percent_rank") >= 0.89, "PREMIUM").otherwise("STANDARD").alias("budget_percent_rank_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("cnt"),
        F.round(F.avg("budget_share"), 2).alias("avg_budget_share"),
    )
    .orderBy(F.col("distinct_ids").desc())
)
