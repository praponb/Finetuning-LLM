from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("sqft").desc())

result = (
    df_stores
    .select("id", "city", "sqft", "region", F.rank().over(w).alias("row_num"))
    .filter(F.col("row_num") == 1)
    .select("id", "city", "sqft", "region")
    .orderBy("sqft", F.col("city").desc())
)
