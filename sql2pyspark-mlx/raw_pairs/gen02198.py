from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .pivot("location", ["Berlin", "London", "Tokyo", "Singapore"])
    .agg(F.max("budget"))
)
