from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_orders
    .filter(F.col("store_id").isNotNull())
    .select(
        "id",
        "amount",
        "discount",
        F.last("discount").over(w).alias("last_discount"),
    )
)
