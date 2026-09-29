from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .pivot("location", ["Bangkok", "Tokyo"])
    .agg(F.avg("budget"))
)
