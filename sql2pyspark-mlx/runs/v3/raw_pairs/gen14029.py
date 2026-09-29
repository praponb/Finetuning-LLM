from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("rating").desc(), "id")
w2 = Window.partitionBy("country").orderBy("rating", "id")

result = (
    df_suppliers
    .filter(F.col("country") == "UK")
    .select(
        "id",
        "rating",
        "country",
        F.row_number().over(w).alias("rn"),
        F.lag("rating", 1).over(w2).alias("prev_rating"),
    )
)
