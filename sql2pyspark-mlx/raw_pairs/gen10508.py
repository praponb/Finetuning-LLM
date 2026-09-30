from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city", "country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_suppliers
    .select(
        "*",
        F.sum("rating").over(w).alias("running_rating"),
        F.sum("rating").over(w2).alias("running_rating_2"),
    )
)

result = (
    scored
    .select("id", "country", "city", "rating")
    .orderBy(F.col("rating").desc_nulls_last())
)
