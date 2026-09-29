from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id")

result = (
    df_suppliers
    .select(
        "id",
        "city",
        "rating",
        F.ntile(4).over(w).alias("rating_ntile"),
        (F.col("rating") - F.lag("rating", 1).over(w)).alias("rating_change"),
    )
    .orderBy("city", "rating_change")
)
