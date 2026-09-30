from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("region").orderBy(F.col("sqft").desc(), "id")
w2 = Window.partitionBy("region", "city").orderBy("sqft", "id")
w_grp = Window.partitionBy("region", "city").orderBy("sqft")

result = (
    df_stores
    .filter(F.col("city").isin(["Singapore", "Berlin"]))
    .select(
        "*",
        F.ntile(10).over(w_ordered).alias("sqft_bucket"),
        F.lag("sqft", 2).over(w2).alias("prev_sqft"),
        F.dense_rank().over(w_grp).alias("sqft_dense_rank"),
    )
)
