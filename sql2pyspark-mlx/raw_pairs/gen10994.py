from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("city", "country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_suppliers
    .select(
        "id",
        "city",
        "rating",
        "country",
        F.round(F.avg("rating").over(w), 2).alias("rating_moving_avg_4"),
        F.sum("rating").over(w2).alias("running_rating"),
    )
    .orderBy("running_rating")
)
