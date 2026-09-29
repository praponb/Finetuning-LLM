from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region")

result = (
    df_stores
    .select(
        "id",
        "region",
        "sqft",
        F.max("sqft").over(w).alias("region_max_sqft"),
        F.min("sqft").over(w).alias("region_min_sqft"),
    )
)
