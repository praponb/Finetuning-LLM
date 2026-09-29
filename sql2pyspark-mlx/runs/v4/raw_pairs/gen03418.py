from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("opened_date_period").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w2 = Window.partitionBy("region", "city").orderBy(F.col("opened_date_period").cast("bigint")).rangeBetween(-604800, Window.currentRow)

base = (
    df_stores
    .filter((F.col("opened_date") >= F.to_date(F.lit("2024-03-05"))) & (F.col("sqft") == 11867))
    .select(
        "*",
        F.date_trunc("month", "opened_date").alias("opened_date_period"),
        F.coalesce("store_name", F.lit("Unknown")).alias("store_name_filled"),
    )
)

result = (
    base
    .select(
        "*",
        F.max("sqft").over(w).alias("sqft_max_7d"),
        F.avg("sqft").over(w2).alias("sqft_avg_7d"),
    )
)
