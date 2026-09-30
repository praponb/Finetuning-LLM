from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .agg((F.max("budget") - F.min("budget")).alias("budget_range"))
    .orderBy("budget_range")
)
