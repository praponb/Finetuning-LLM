from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("YEAR", "order_date"), "customer_id").orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("customer_id").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_orders
    .select(
        "id",
        "customer_id",
        "channel",
        "shipping_fee",
        F.last("discount", ignorenulls=True).over(w).alias("last_known_discount"),
        F.last("channel", ignorenulls=True).over(w_ordered).alias("last_known_channel"),
    )
)
