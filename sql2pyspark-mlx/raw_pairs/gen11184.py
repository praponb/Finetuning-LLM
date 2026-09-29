from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.partitionBy("region")

result = (
    df_stores
    .filter(F.col("city") == "Tokyo")
    .select(
        "id",
        "city",
        "sqft",
        "region",
        F.sum("sqft").over(w).alias("city_sum_sqft"),
        F.max("sqft").over(w2).alias("region_max_sqft"),
    )
)
