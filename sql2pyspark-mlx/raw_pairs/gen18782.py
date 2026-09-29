from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("sqft").asc_nulls_first())
w2 = Window.partitionBy("region").orderBy(F.col("sqft").asc_nulls_first())
w3 = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_stores
    .filter(F.lower(F.trim("store_name")) == "old town")
    .select(
        "id",
        "city",
        "region",
        F.cume_dist().over(w).alias("sqft_cume_dist"),
        F.rank().over(w2).alias("sqft_rank"),
        F.avg("sqft").over(w3).alias("sqft_avg_6"),
    )
)
