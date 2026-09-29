from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id")
w2 = Window.partitionBy("country", "city").orderBy("rating", "id")

result = (
    df_suppliers
    .select(
        "id",
        "city",
        "rating",
        "country",
        F.lag("rating", 2).over(w).alias("prev_rating"),
        F.sum("rating").over(w2).alias("running_rating"),
    )
    .orderBy(F.col("prev_rating").desc())
)
