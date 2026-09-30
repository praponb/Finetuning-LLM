from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")

result = (
    df_stores
    .filter(F.col("store_name").isNotNull())
    .select("id", "city", "sqft", F.avg("sqft").over(w).alias("city_avg_sqft"))
    .orderBy("city", F.col("sqft").desc())
)
