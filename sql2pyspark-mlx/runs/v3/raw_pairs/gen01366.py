from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") >= 1744000)
    .groupBy("location")
    .pivot("dept_name", ["Legal", "Marketing", "Sales", "Engineering"])
    .agg(F.avg("budget"))
    .orderBy("location")
)
