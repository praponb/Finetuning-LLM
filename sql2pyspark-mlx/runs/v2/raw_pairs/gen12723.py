from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("city", "country")

result = (
    df_suppliers
    .filter(F.col("rating") > 3.3)
    .select(
        "id",
        "country",
        "rating",
        "city",
        F.round(F.avg("rating").over(w), 2).alias("rating_moving_avg_4"),
        F.round((F.col("rating") / F.sum("rating").over(w2)), 3).alias("rating_share"),
    )
    .orderBy("city")
)
