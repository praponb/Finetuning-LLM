from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id")
w2 = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("dept_name")
w4 = Window.partitionBy("dept_name").orderBy("budget", "id")
w_ordered = Window.partitionBy("location").orderBy(F.col("budget").asc_nulls_last())

filtered = (
    df_departments
    .filter(F.col("location").isNotNull())
    .withColumn("location_dept_name", F.concat("location", F.lit(" - "), "dept_name"))
    .withColumn("resolved_dept_name", F.coalesce("dept_name", F.lit("N/A")))
)

scored = (
    filtered
    .withColumn("prev_budget", F.lag("budget", 2).over(w))
    .withColumn("cumulative_budget", F.sum("budget").over(w2))
    .withColumn("dept_name_min_budget", F.min("budget").over(w_part))
    .withColumn("prev_budget_2", F.lag("budget", 1, 0).over(w4))
    .withColumn("budget_dense_rank", F.dense_rank().over(w_ordered))
)

eligible = (
    scored
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.dept_id").isin([2, 4, 21])), "left_anti")
)

result = (
    eligible
    .groupBy(
        F.when(F.col("prev_budget") >= 3242000, "PREMIUM").otherwise("STANDARD").alias("prev_budget_tier"),
    )
    .agg(
        F.count(F.when(F.col("dept_name") == "Marketing", 1)).alias("marketing_count"),
        F.count(F.when(F.col("dept_name") == "HR", 1)).alias("hr_rows"),
    )
    .filter(F.col("marketing_count") > 1)
)
