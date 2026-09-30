from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name").orderBy("budget_scaled", "id")
w2 = Window.partitionBy("location", "dept_name").orderBy(F.col("budget").asc_nulls_first())
w3 = Window.partitionBy("location").orderBy(F.col("budget").desc_nulls_first(), "id")

filtered = (
    df_departments
    .filter(F.col("dept_name").isNotNull() & F.col("budget").isNotNull())
    .select(
        "*",
        F.round((F.col("budget") * 1.07), 2).alias("budget_scaled"),
        F.coalesce("dept_name", F.lit("UNKNOWN")).alias("resolved_dept_name"),
    )
)

with_windows = (
    filtered
    .filter(F.col("budget") > 3034000)
    .withColumn("next_budget", F.lead("budget", 1, 0).over(w))
    .withColumn("budget_percent_rank", F.percent_rank().over(w2))
)

eligible = (
    with_windows
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.is_remote")), "left_anti")
)

latest = (
    eligible
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

totals = (
    latest
    .groupBy(
        F.when(F.col("budget_percent_rank") >= 0.63, "PREMIUM").otherwise("STANDARD").alias("budget_percent_rank_tier"),
    )
    .agg(
        F.count(F.when(F.col("location") == "New York", 1)).alias("new_york_count"),
        F.sum("next_budget").alias("total_next_budget"),
    )
)

result = totals.select("budget_percent_rank_tier", "new_york_count", "total_next_budget")
