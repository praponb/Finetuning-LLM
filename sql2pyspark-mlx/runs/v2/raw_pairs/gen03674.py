from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.partitionBy("region")

result = (
    df_stores
    .select(
        "id",
        "city",
        "sqft",
        "region",
        F.avg("sqft").over(w).alias("city_avg_sqft"),
        F.count("*").over(w2).alias("region_count"),
    )
    .orderBy("city")
)
