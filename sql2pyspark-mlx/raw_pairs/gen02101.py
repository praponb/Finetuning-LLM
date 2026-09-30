from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("opened_date", "id")
w2 = Window.partitionBy("region").orderBy(F.col("sqft").desc())

result = (
    df_stores
    .filter(F.col("sqft") != 6101)
    .select(
        "id",
        "region",
        "opened_date",
        "sqft",
        (F.col("sqft") - F.lag("sqft", 2).over(w)).alias("sqft_change"),
        F.dense_rank().over(w2).alias("sqft_dense_rank"),
    )
)
