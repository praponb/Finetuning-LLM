from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Finance", "Sales", "HR"]))
    .groupBy("dept_name")
    .pivot("location", ["Tokyo", "Singapore"])
    .agg(F.avg("budget"))
    .orderBy("dept_name")
)
