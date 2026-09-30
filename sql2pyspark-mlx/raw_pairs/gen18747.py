from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating", "id")

result = (
    df_suppliers
    .select(
        "id",
        "country",
        "rating",
        (F.col("rating") - F.lag("rating", 1, 0).over(w)).alias("rating_change"),
    )
    .orderBy("rating_change", F.col("id").desc())
)
