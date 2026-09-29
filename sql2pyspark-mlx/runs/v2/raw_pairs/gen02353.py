from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("sqft")

result = (
    df_stores
    .filter(F.year("opened_date") == 2025)
    .select("id", "city", "sqft", F.dense_rank().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 3)
    .select("id", "city", "sqft")
    .orderBy(F.col("city").desc())
)
