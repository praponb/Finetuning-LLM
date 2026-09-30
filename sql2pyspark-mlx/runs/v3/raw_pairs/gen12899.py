from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("sqft")
w2 = Window.partitionBy("city")

result = (
    df_stores
    .filter(F.col("store_name").like("%Air%"))
    .select(
        "id",
        "region",
        "sqft",
        "city",
        F.percent_rank().over(w).alias("sqft_pct_rank"),
        F.max("sqft").over(w2).alias("city_max_sqft"),
    )
)
