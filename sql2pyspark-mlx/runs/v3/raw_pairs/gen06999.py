from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating", "id")
w2 = Window.partitionBy("country").orderBy(F.col("rating").desc())

result = (
    df_suppliers
    .filter(F.col("rating") <= 2.4)
    .select(
        "id",
        "country",
        "rating",
        F.lag("rating", 1).over(w).alias("prev_rating"),
        F.rank().over(w2).alias("rnk"),
    )
)
