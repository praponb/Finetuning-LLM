from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region", "city")
w2 = Window.partitionBy("city", "region").orderBy(F.col("sqft").desc())

result = (
    df_stores
    .filter(F.col("store_name").like("Si%"))
    .select(
        "id",
        "region",
        "city",
        "sqft",
        F.count("*").over(w).alias("region_count"),
        F.dense_rank().over(w2).alias("dense_rnk"),
    )
)
