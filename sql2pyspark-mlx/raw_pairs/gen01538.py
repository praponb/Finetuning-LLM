from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.partitionBy("region")
w3 = Window.partitionBy(F.month("opened_date")).orderBy("sqft", "id")
w4 = Window.partitionBy("region").orderBy("opened_date", "id")

result = (
    df_stores
    .select(
        "id",
        "city",
        "region",
        "sqft",
        F.min("sqft").over(w).alias("city_min_sqft"),
        F.round((F.col("sqft") / F.sum("sqft").over(w2)), 4).alias("sqft_share"),
        F.ntile(4).over(w3).alias("sqft_bucket"),
        F.lead("sqft", 1).over(w4).alias("next_sqft"),
    )
)
