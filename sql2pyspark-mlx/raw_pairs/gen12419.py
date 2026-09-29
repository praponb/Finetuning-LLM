from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .pivot("dept_name", ["Engineering", "Marketing", "HR"])
    .agg(F.avg("budget").alias("amount"), F.count(F.lit(1)).alias("cnt"))
)
