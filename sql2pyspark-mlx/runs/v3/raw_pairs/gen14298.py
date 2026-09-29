from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "store_id").orderBy(F.col("amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_orders
    .filter(F.col("shipping_fee").isNotNull())
    .select(
        "id",
        "customer_id",
        "store_id",
        "amount",
        "shipping_fee",
        F.last("shipping_fee").over(w).alias("last_shipping_fee"),
    )
)
