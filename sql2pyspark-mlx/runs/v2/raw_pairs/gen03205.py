from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_stores
    .filter(F.col("sqft") == 7134)
    .select(
        "id",
        "opened_date",
        "store_name",
        F.last("store_name").over(w).alias("last_store_name"),
    )
    .orderBy("last_store_name")
)
