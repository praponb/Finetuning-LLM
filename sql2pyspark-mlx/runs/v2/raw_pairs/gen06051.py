from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name") != "Finance")
    .groupBy("dept_name")
    .agg((F.max("budget") - F.min("budget")).alias("budget_range"))
    .filter(F.col("budget_range") < 51)
    .orderBy("dept_name", F.col("budget_range").desc())
)
