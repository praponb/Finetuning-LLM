from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").desc())

result = (
    df_departments
    .select("id", "location", "budget", F.dense_rank().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 2)
    .select("id", "location", "budget")
    .orderBy("budget")
)
