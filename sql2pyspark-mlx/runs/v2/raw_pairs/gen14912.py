from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .pivot("location", ["New York", "Bangkok", "Singapore"])
    .agg(F.avg("budget"))
)
