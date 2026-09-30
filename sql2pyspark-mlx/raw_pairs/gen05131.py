from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("location") == "Tokyo") & (F.col("budget") >= 3437000))
    .groupBy("location")
    .agg(F.count(F.when(F.col("dept_name") == "Finance", 1)).alias("finance_count"))
    .orderBy("finance_count", "location")
)
