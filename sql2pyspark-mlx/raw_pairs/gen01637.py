from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id")
w2 = Window.partitionBy("country").orderBy(F.col("rating").desc(), "id")

result = (
    df_suppliers
    .select(
        "id",
        "city",
        "rating",
        "country",
        (F.col("rating") - F.lag("rating", 1, 0).over(w)).alias("rating_change"),
        F.first("city").over(w2).alias("first_city"),
    )
    .orderBy(F.col("rating").desc())
)
