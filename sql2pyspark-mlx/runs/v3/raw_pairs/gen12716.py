from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").desc())
w2 = Window.orderBy(F.col("rating").desc())

result = (
    df_suppliers
    .filter(F.col("rating") >= 3.4)
    .select(
        "id",
        "city",
        "rating",
        F.dense_rank().over(w).alias("rating_dense_rank"),
        F.dense_rank().over(w2).alias("dense_rnk"),
    )
)
