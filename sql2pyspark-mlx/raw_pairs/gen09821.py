from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_stores
    .select(
        "id",
        "city",
        "sqft",
        "region",
        "opened_date",
        F.sum("sqft").over(w).alias("city_sum_sqft"),
        F.last("sqft").over(w2).alias("last_sqft"),
    )
)
