from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("sqft").desc(), "id")
w2 = Window.partitionBy("region")
w3 = Window.partitionBy("region", "city").orderBy("opened_date", "id")
w4 = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy(F.col("sqft").desc_nulls_last())

result = (
    df_stores
    .select(
        "*",
        F.ntile(5).over(w).alias("sqft_bucket"),
        F.min("sqft").over(w2).alias("region_min_sqft"),
        F.lag("sqft", 2).over(w3).alias("prev_sqft"),
        F.first("region", ignorenulls=True).over(w4).alias("first_known_region"),
        F.dense_rank().over(w5).alias("sqft_dense_rank"),
    )
)
