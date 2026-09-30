from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("discount", "id")
w2 = Window.partitionBy("status").orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_orders
    .select(
        "*",
        F.ntile(10).over(w).alias("discount_bucket"),
        F.last("channel", ignorenulls=True).over(w2).alias("last_known_channel"),
    )
)

result = (
    enriched
    .select(
        "id",
        "store_id",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        "discount_bucket",
        "last_known_channel",
    )
    .orderBy(F.col("store_id").desc_nulls_first())
)
