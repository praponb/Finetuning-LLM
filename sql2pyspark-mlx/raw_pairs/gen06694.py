from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("order_date")).orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy(F.col("amount").desc(), "id")
w3 = Window.partitionBy("customer_id")

base = (
    df_orders
    .filter(F.col("shipping_fee") <= 27)
    .withColumn("discount_times_shipping_fee", (F.col("discount") * F.col("shipping_fee")))
)

result = (
    base
    .select(
        "id",
        "customer_id",
        "store_id",
        "status",
        "shipping_fee",
        "discount_times_shipping_fee",
        F.last("shipping_fee", ignorenulls=True).over(w).alias("last_known_shipping_fee"),
        F.ntile(4).over(w2).alias("amount_bucket"),
        F.max("shipping_fee").over(w3).alias("customer_id_max_shipping_fee"),
    )
)
