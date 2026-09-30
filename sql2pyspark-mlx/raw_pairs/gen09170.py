from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("store_id").orderBy("discount", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("channel").orderBy("amount", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_orders
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "amount",
        "discount",
        "shipping_fee",
        F.min("amount").over(w_hist).alias("last3_min_amount"),
        F.avg("amount").over(w_hist).alias("last3_avg_amount"),
        F.sum("amount").over(w_hist).alias("last3_sum_amount"),
        F.max("amount").over(w_hist).alias("last3_max_amount"),
        F.avg("discount").over(w2).alias("discount_avg_6"),
    )
)
