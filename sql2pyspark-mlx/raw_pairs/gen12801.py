from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").desc_nulls_last(), "id")

latest = (
    df_departments
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "dept_name", "location", "budget")
)

result = (
    latest
    .select("id", "dept_name", "location", "budget")
    .orderBy(F.col("budget").asc_nulls_last(), F.col("location").desc())
)
