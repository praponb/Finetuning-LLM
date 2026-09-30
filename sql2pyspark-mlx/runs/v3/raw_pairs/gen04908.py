from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").between(3094000, 3140000))
    .groupBy("dept_name", "location")
    .agg(F.sum("budget").alias("total_budget"))
    .filter(F.col("total_budget") < 9162000)
    .orderBy("location", "dept_name")
    .limit(20)
)
