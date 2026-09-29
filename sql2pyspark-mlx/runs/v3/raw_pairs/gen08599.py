from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") < 3082000)
    .groupBy("dept_name")
    .agg((F.max("budget") - F.min("budget")).alias("budget_range"))
    .filter(F.col("budget_range") < 29)
    .orderBy(F.col("budget_range").desc())
)
