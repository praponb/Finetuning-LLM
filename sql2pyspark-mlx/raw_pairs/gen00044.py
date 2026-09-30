from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("sqft").desc())
w2 = Window.partitionBy("region").orderBy(F.col("sqft").desc(), "id")

result = (
    df_stores
    .filter(F.col("city").isin(["Berlin", "Singapore"]))
    .select(
        "id",
        "region",
        "sqft",
        F.dense_rank().over(w).alias("dense_rnk"),
        F.row_number().over(w2).alias("sqft_row"),
    )
)
