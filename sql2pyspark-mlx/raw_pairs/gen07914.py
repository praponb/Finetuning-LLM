from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region")
w2 = Window.partitionBy("city")

result = (
    df_stores
    .filter(F.col("city") == "Singapore")
    .select(
        "id",
        "region",
        "sqft",
        "city",
        F.max("sqft").over(w).alias("region_max_sqft"),
        F.count("*").over(w2).alias("city_count"),
    )
)
