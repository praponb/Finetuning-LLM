from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id")

result = (
    df_suppliers
    .select(
        "id",
        "city",
        "rating",
        (F.col("rating") - F.lag("rating", 1).over(w)).alias("rating_change"),
    )
    .orderBy(F.col("id").desc(), F.col("rating_change").desc())
)
