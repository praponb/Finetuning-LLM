from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("rating", "id").rowsBetween(-5, Window.currentRow)

scored = (
    df_suppliers
    .select(
        "id",
        "city",
        "rating",
        F.sum("rating").over(w).alias("running_rating"),
        F.avg("rating").over(w2).alias("rating_avg_5"),
    )
)

result = (
    scored
    .select("id", "running_rating", "rating_avg_5")
    .orderBy(F.col("running_rating").asc_nulls_last(), F.col("id").desc())
)
