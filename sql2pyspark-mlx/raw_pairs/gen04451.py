from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").desc(), "id")

ranked = (
    df_departments
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    ranked
    .select("id", "dept_name", "location", "budget", "dedup_rank")
    .orderBy(F.col("dedup_rank").desc_nulls_last())
)
