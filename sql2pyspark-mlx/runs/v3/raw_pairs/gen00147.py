from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("rating").desc())

result = (
    df_suppliers
    .filter(F.col("rating") >= 4.2)
    .select("id", "rating", F.dense_rank().over(w).alias("rating_dense_rank"))
    .orderBy("rating_dense_rank")
)
