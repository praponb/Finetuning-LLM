from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region", "city")

result = (
    df_stores
    .filter(F.col("city") == "London")
    .select(
        "id",
        "region",
        "city",
        "sqft",
        F.avg("sqft").over(w).alias("region_avg_sqft"),
    )
    .orderBy("region", F.col("sqft").desc())
)
