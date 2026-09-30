from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.partitionBy("region", "city").orderBy("opened_date", "id")

result = (
    df_stores
    .filter(F.col("city") == "Bangkok")
    .select(
        "id",
        "city",
        "sqft",
        "region",
        "opened_date",
        F.avg("sqft").over(w).alias("city_avg_sqft"),
        F.lead("sqft", 2).over(w2).alias("next_sqft"),
    )
)
