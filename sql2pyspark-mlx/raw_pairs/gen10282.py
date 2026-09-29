from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("sqft").desc_nulls_first())
w_running = Window.orderBy("sqft", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("city").orderBy("sqft")
w4 = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("city", "region")

result = (
    df_stores
    .filter(F.col("opened_date") >= "2025-05-26")
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        F.rank().over(w).alias("sqft_rank"),
        F.avg("sqft").over(w_running).alias("sqft_avg_2"),
        F.dense_rank().over(w3).alias("sqft_dense_rank"),
        F.sum("sqft").over(w4).alias("cumulative_sqft"),
        F.sum("sqft").over(w5).alias("city_sum_sqft"),
    )
)
