from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("rating").desc())
w2 = Window.partitionBy("city").orderBy("rating", "id")

result = (
    df_suppliers
    .filter(F.col("country") == "US")
    .select(
        "id",
        "rating",
        "city",
        F.dense_rank().over(w).alias("dense_rnk"),
        F.lag("rating", 1).over(w2).alias("prev_rating"),
    )
)
