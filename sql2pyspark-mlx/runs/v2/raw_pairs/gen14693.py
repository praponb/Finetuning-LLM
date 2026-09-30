from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name") != "Legal")
    .groupBy("dept_name")
    .pivot("location", ["Berlin", "Tokyo", "Singapore"])
    .agg(F.avg("budget"))
    .orderBy("dept_name")
)
