from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("country", "city").orderBy(F.col("rating").desc(), "id")

metrics = (
    df_suppliers
    .select(
        "id",
        "city",
        F.last("rating", ignorenulls=True).over(w).alias("last_known_rating"),
        F.ntile(4).over(w2).alias("rating_bucket"),
    )
)

result = (
    metrics
    .select("id", "last_known_rating", "rating_bucket")
    .orderBy("rating_bucket", "last_known_rating")
)
