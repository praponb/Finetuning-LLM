from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("customer_id").orderBy("shipping_fee", "id")
w2 = Window.orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_orders
    .select(
        "id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        F.lag("amount").over(w_ordered).alias("prev_amount"),
        F.lead("amount").over(w_ordered).alias("next_amount"),
        F.lag("shipping_fee").over(w_ordered).alias("prev_shipping_fee"),
        F.row_number().over(w_ordered).alias("row_seq"),
        F.sum("discount").over(w2).alias("running_discount"),
    )
)

result = (
    metrics
    .select(
        "id",
        "order_date",
        "status",
        "channel",
        "discount",
        "prev_amount",
        "next_amount",
        "row_seq",
        "running_discount",
        F.when(F.col("channel").rlike("^(web|store)$"), "EXTENDED").when(F.col("channel").rlike("^(mobile|partner)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("channel_zone"),
    )
    .orderBy(F.col("order_date").desc())
)
