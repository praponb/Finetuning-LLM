from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w2 = Window.partitionBy("city").orderBy("opened_date")

result = (
    df_stores
    .filter(F.year("opened_date") >= 2024)
    .select(
        "id",
        "opened_date",
        "store_name",
        "city",
        F.last("store_name").over(w).alias("last_store_name"),
        F.rank().over(w2).alias("rnk"),
    )
)
