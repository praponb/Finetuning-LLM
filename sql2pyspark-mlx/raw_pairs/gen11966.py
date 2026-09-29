from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .pivot("location", ["New York", "London", "Tokyo", "Chiang Mai"])
    .agg(F.avg("budget"))
    .orderBy("dept_name")
)
