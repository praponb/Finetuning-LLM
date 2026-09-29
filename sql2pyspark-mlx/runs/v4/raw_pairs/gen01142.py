from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("rating", "id")
w2 = Window.partitionBy("city").orderBy(F.col("rating").desc())

result = (
    df_suppliers
    .select(
        "id",
        "rating",
        "city",
        F.lag("rating", 1).over(w).alias("prev_rating"),
        F.dense_rank().over(w2).alias("dense_rnk"),
    )
    .orderBy(F.col("dense_rnk").desc())
)
