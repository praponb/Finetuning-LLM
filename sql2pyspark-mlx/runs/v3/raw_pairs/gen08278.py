from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("opened_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w2 = Window.partitionBy("region").orderBy("opened_date", "id")

result = (
    df_stores
    .select(
        "id",
        "city",
        "opened_date",
        "region",
        "sqft",
        F.last("region").over(w).alias("last_region"),
        F.sum("sqft").over(w2).alias("running_sqft"),
    )
    .orderBy("opened_date")
)
