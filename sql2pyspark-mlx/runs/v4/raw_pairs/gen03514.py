from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name", "location").orderBy(F.col("budget").desc_nulls_last())
w2 = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(-2, Window.currentRow)

with_windows = (
    df_departments
    .select(
        "*",
        F.rank().over(w).alias("budget_rank"),
        F.avg("budget").over(w2).alias("budget_avg_2"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("budget_avg_2") >= 3864974, "LARGE").otherwise("SMALL").alias("budget_avg_2_tier"),
    )
    .agg(
        F.round(F.sum("budget") / F.nullif(F.count(F.when(F.col("dept_name") == "Sales", 1)), F.lit(0)), 2).alias("budget_per_sales"),
        F.count(F.when(F.col("location") == "London", 1)).alias("london_count"),
        F.sum("budget").alias("total_budget"),
        F.percentile_approx("budget_rank", [0.25, 0.5, 0.75]).alias("budget_rank_quartiles"),
        F.count_if(F.col("location") == "New York").alias("new_york_events"),
    )
    .orderBy("new_york_events")
)
