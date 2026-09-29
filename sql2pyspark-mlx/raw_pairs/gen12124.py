from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("status").orderBy("shipping_fee", "id")

result = (
    df_orders
    .filter(F.col("customer_id").isin([7, 22, 36]))
    .select(
        "id",
        "order_date",
        "channel",
        "amount",
        "discount",
        F.lag("amount").over(w_hist).alias("prev_amount"),
        F.lead("amount").over(w_hist).alias("next_amount"),
        F.lag("shipping_fee").over(w_hist).alias("prev_shipping_fee"),
        F.lead("shipping_fee").over(w_hist).alias("next_shipping_fee"),
        F.row_number().over(w_hist).alias("event_seq"),
    )
)
