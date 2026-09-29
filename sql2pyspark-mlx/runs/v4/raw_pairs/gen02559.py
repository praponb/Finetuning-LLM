from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy(F.col("rating_whole").asc_nulls_last())
w2 = Window.partitionBy("country").orderBy("rating", "id")
w3 = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.partitionBy("country").orderBy(F.col("rating_whole").asc_nulls_first())
w5 = Window.orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select("*", F.ceil("rating").alias("rating_whole"))
)

result = (
    base
    .select(
        "*",
        F.rank().over(w).alias("rating_whole_rank"),
        F.lag("rating_whole", 1, 0).over(w2).alias("prev_rating_whole"),
        F.avg("rating").over(w3).alias("rating_avg_2"),
        F.dense_rank().over(w4).alias("rating_whole_dense_rank"),
        F.last("rating", ignorenulls=True).over(w5).alias("last_known_rating"),
    )
)
