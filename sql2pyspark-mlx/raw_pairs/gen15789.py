from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location") == "Berlin")
    .groupBy("dept_name")
    .agg((F.max("budget") - F.min("budget")).alias("budget_range"))
    .filter(F.col("budget_range") > 32)
)
