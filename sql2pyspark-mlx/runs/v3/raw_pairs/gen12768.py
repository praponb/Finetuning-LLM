from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region", "city")

result = (
    df_stores
    .select(
        "id",
        "region",
        "city",
        "sqft",
        F.min("sqft").over(w).alias("region_min_sqft"),
    )
)
