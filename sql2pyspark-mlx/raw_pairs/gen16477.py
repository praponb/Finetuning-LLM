from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("location")

with_windows = (
    df_departments
    .filter(F.col("budget").between(1234000, 2704000))
    .select(
        "id",
        "dept_name",
        "location",
        F.min("budget").over(w).alias("last7_min_budget"),
        F.avg("budget").over(w).alias("last7_avg_budget"),
        F.max("budget").over(w).alias("last7_max_budget"),
        F.sum("budget").over(w).alias("last7_sum_budget"),
        F.round((F.col("budget") / F.sum("budget").over(w2)), 4).alias("budget_share"),
    )
)

result = (
    with_windows
    .groupBy("dept_name")
    .agg(
        F.count(F.when(F.col("location") == "Bangkok", 1)).alias("bangkok_rows"),
        F.round(F.avg("last7_sum_budget"), 2).alias("avg_last7_sum_budget"),
        F.round(F.sum("budget_share") / F.nullif(F.count(F.when(F.col("location") == "London", 1)), F.lit(0)), 2).alias("budget_share_per_london"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("avg_last7_sum_budget").desc())
)
