from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("opened_date")

result = (
    df_stores
    .select("id", "city", "opened_date", F.dense_rank().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 2)
    .select("id", "city", "opened_date")
    .orderBy(F.col("id").desc())
)
