from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region")

result = (
    df_stores
    .filter(F.col("sqft") > 6229)
    .select("id", "region", "sqft", F.sum("sqft").over(w).alias("region_sum_sqft"))
    .orderBy(F.col("id").desc())
)
