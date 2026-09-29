from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy("rating", "id")

result = (
    df_suppliers
    .filter(F.col("supplier_name").isNotNull())
    .select(
        "id",
        "city",
        "country",
        "rating",
        F.lag("rating", 1).over(w).alias("prev_rating"),
    )
)
