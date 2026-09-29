from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("sqft").desc())
w2 = Window.partitionBy("region").orderBy("opened_date", "id")

result = (
    df_stores
    .select(
        "id",
        "region",
        "sqft",
        "opened_date",
        F.dense_rank().over(w).alias("sqft_dense_rank"),
        F.row_number().over(w2).alias("row_num"),
    )
)
