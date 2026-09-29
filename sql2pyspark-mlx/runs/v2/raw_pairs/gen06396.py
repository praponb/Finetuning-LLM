from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")

result = (
    df_stores
    .filter(F.col("sqft").between(11840, 14115))
    .select("id", "city", "sqft", F.avg("sqft").over(w).alias("city_avg_sqft"))
)
