from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region")

result = (
    df_stores
    .filter(F.col("store_name").isNotNull())
    .select("id", "region", "sqft", F.sum("sqft").over(w).alias("region_sum_sqft"))
)
