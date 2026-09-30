from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") <= 3650000)
    .groupBy("dept_name")
    .pivot("location", ["London", "Singapore"])
    .agg(F.count("budget"))
)
