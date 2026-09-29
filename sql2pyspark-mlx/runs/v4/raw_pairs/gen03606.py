from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city").orderBy(F.col("sqft").asc_nulls_last())
w3 = Window.partitionBy("city", "region").orderBy("sqft", "id")
w4 = Window.partitionBy("region")
w5 = Window.partitionBy("city").orderBy("sqft", "id")

result = (
    df_stores
    .select(
        "*",
        F.sum("sqft").over(w).alias("running_sqft"),
        F.rank().over(w2).alias("sqft_rank"),
        F.ntile(3).over(w3).alias("sqft_bucket"),
        F.sum("sqft").over(w4).alias("region_sum_sqft"),
        F.lag("sqft", 1, 0).over(w5).alias("prev_sqft"),
    )
)
