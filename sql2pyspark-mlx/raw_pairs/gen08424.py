from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") < 1959000)
    .groupBy("dept_name")
    .pivot("location", ["Berlin", "Singapore", "Bangkok", "London"])
    .agg(F.sum("budget"))
    .orderBy("dept_name")
)
