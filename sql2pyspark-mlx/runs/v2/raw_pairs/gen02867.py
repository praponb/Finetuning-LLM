from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("sqft").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_stores
    .filter(F.col("opened_date") < "2025-03-01")
    .select(
        "id",
        "city",
        "sqft",
        "region",
        F.last("region").over(w).alias("last_region"),
    )
)
