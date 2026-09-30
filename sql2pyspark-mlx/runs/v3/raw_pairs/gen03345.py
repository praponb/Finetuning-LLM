from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("order_date", "id")

result = (
    df_orders
    .select(
        "id",
        "store_id",
        "order_date",
        "discount",
        "shipping_fee",
        (F.col("discount") - F.lag("discount", 1, 0).over(w)).alias("discount_change"),
        F.lead("shipping_fee", 1).over(w).alias("next_shipping_fee"),
    )
    .orderBy(F.col("next_shipping_fee").desc())
)
