from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").desc())

result = (
    df_departments
    .filter(F.col("location") == "Chiang Mai")
    .select("id", "location", "budget", F.dense_rank().over(w).alias("rn"))
    .filter(F.col("rn") <= 5)
    .select("id", "location", "budget")
)
