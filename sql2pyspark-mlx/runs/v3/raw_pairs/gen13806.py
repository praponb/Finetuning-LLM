from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location") == "Singapore")
    .groupBy("dept_name")
    .pivot("location", ["Singapore", "New York", "Tokyo", "Chiang Mai"])
    .agg(F.avg("budget"))
)
