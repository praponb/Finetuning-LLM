from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_suppliers
    .filter(F.col("rating") > 2.1)
    .select(
        "id",
        "country",
        "rating",
        "city",
        F.last("city").over(w).alias("last_city"),
    )
)
