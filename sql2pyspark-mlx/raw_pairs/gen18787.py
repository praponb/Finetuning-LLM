from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget")

result = (
    df_departments
    .filter(F.col("location") == "Tokyo")
    .select("id", "location", "budget", F.dense_rank().over(w).alias("dense_rnk"))
)
