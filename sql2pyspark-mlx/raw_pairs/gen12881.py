from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("sqft")
w2 = Window.partitionBy("city").orderBy(F.col("sqft").desc(), "id")
w3 = Window.orderBy(F.col("sqft").desc(), "id")

valid = (
    df_stores
    .filter(F.col("opened_date") >= F.lit("2023-06-24").cast("date"))
    .select(
        "id",
        "store_name",
        "city",
        "opened_date",
        "sqft",
        F.concat_ws("/", "city", "store_name").alias("city_store_name_key"),
    )
)

result = (
    valid
    .select(
        "id",
        "store_name",
        "city",
        "city_store_name_key",
        F.rank().over(w).alias("sqft_rank"),
        F.ntile(4).over(w2).alias("sqft_bucket"),
        F.ntile(5).over(w3).alias("sqft_bucket_2"),
    )
)
