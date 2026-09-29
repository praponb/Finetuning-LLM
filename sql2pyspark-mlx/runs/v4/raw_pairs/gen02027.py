from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .pivot("location", ["New York", "Berlin", "Singapore", "Tokyo"])
    .agg(F.max("budget"))
)
