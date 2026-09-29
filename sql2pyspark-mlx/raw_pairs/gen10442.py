from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").desc_nulls_last(), "id")

ranked = (
    df_departments
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    ranked
    .select("id", "dept_name", "budget", "latest_rank")
    .orderBy(F.col("latest_rank").asc_nulls_last())
)
