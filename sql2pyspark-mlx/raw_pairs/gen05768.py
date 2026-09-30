from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("discount", "id")
w2 = Window.orderBy("order_date", "id")

result = (
    df_orders
    .select(
        "id",
        "store_id",
        "status",
        "channel",
        "amount",
        "discount",
        F.lead("amount").over(w).alias("next_amount"),
        F.lag("amount").over(w).alias("prev_amount"),
        F.lag("shipping_fee").over(w).alias("prev_shipping_fee"),
        F.lead("shipping_fee").over(w).alias("next_shipping_fee"),
        F.row_number().over(w).alias("seq_no"),
        F.lag("amount", 1).over(w2).alias("prev_amount_2"),
    )
)
