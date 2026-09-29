from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("sqft")
w2 = Window.partitionBy("region").orderBy("sqft", "id")

result = (
    df_stores
    .select(
        "id",
        "sqft",
        "region",
        F.dense_rank().over(w).alias("dense_rnk"),
        F.row_number().over(w2).alias("sqft_row"),
    )
)
