from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id")

latest = (
    df_departments
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "dept_name", "budget")
)

result = (
    latest
    .withColumn("budget_tier", F.when(F.col("budget") >= 4086000, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["dept_name", "budget_tier"], ["dept_name"], []],
        "dept_name",
        "budget_tier",
    )
    .agg(
        F.count(F.when(F.col("dept_name") == "Finance", 1)).alias("finance_count"),
        F.round(F.avg("budget"), 2).alias("avg_budget"),
        F.count("*").alias("cnt"),
        F.stddev_samp("budget").alias("budget_stddev"),
        F.count(F.when(F.col("dept_name") == "HR", 1)).alias("hr_rows"),
        F.min("budget").alias("min_budget"),
    )
    .orderBy(F.col("avg_budget").desc_nulls_first())
)
