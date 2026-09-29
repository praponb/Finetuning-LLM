from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("sqft").desc())

result = (
    df_stores
    .filter(F.col("store_name").like("%Centra%"))
    .select("id", "city", "sqft", "opened_date", F.rank().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 2)
    .select("id", "city", "sqft", "opened_date")
    .orderBy(F.col("sqft").desc())
)
