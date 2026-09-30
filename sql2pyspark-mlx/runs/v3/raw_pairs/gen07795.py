from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_suppliers
    .select(
        "id",
        "city",
        "rating",
        "country",
        F.last("country").over(w).alias("last_country"),
    )
)
