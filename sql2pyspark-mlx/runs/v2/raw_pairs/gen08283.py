from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_orders
    .filter(F.col("order_date") >= "2024-02-12")
    .select(
        "id",
        "status",
        "amount",
        "discount",
        F.last("discount").over(w).alias("last_discount"),
    )
)
