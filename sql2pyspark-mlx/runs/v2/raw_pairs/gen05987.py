from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_suppliers
    .filter(F.col("city") != "London")
    .select(
        "id",
        "city",
        "rating",
        "country",
        F.last("country").over(w).alias("last_country"),
    )
)
