from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy(F.col("opened_date").desc())

latest = (
    df_stores
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "city", "region", "sqft")
)

result = (
    latest
    .select("id", "city", "sqft")
    .orderBy(F.col("id").desc())
    .limit(5)
)
