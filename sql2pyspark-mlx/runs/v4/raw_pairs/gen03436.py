from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("rating", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.orderBy(F.col("rating").desc(), "id")

result = (
    df_suppliers
    .filter(F.col("rating") < 2.0)
    .select(
        "id",
        "rating",
        "country",
        F.round(F.avg("rating").over(w), 2).alias("rating_moving_avg_7"),
        F.first("country").over(w2).alias("first_country"),
    )
)
