from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .pivot("location", ["New York", "Chiang Mai", "Singapore"])
    .agg(F.avg("budget"))
    .orderBy("dept_name")
)
