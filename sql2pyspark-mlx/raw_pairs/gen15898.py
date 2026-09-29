from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .pivot("location", ["Chiang Mai", "New York", "London", "Tokyo"])
    .agg(F.avg("budget"))
)
