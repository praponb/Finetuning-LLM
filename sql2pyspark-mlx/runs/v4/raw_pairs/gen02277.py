from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w2 = Window.orderBy("opened_date", "id")

result = (
    df_stores
    .select(
        "id",
        "city",
        "sqft",
        "store_name",
        "opened_date",
        F.last("store_name").over(w).alias("last_store_name"),
        F.sum("sqft").over(w2).alias("running_sqft"),
    )
)
