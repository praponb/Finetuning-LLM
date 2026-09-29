from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name") == "Sales")
    .groupBy("dept_name")
    .pivot("location", ["Singapore", "New York"])
    .agg(F.avg("budget"))
    .orderBy("dept_name")
)
