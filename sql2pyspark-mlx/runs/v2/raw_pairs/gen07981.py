from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id")
w2 = Window.partitionBy("city")

result = (
    df_suppliers
    .filter(F.col("country") != "DE")
    .select(
        "id",
        "city",
        "rating",
        F.lag("rating", 1, 0).over(w).alias("prev_rating"),
        F.min("rating").over(w2).alias("city_min_rating"),
    )
    .orderBy("prev_rating", "city_min_rating")
)
