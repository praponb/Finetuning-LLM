from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("opened_date").desc())

result = (
    df_stores
    .filter(F.col("store_name").like("%Tow%"))
    .select("id", "city", "opened_date", "sqft", F.rank().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 3)
    .select("id", "city", "opened_date", "sqft")
)
