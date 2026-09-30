from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("sqft").desc())
w2 = Window.partitionBy("region").orderBy("opened_date", "id")

result = (
    df_stores
    .select(
        "id",
        "city",
        "sqft",
        "region",
        "opened_date",
        "store_name",
        F.dense_rank().over(w).alias("sqft_dense_rank"),
        F.first("store_name").over(w2).alias("first_store_name"),
    )
    .orderBy("sqft")
)
