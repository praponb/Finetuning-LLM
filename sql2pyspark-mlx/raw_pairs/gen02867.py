from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("amount", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy("shipping_fee", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("customer_id")
w4 = Window.partitionBy("status", "store_id").orderBy("discount", "id").rowsBetween(-3, -1)

result = (
    df_orders
    .filter(F.col("channel").isin(["partner", "mobile", "store"]))
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.avg("discount").over(w).alias("discount_avg_2"),
        F.avg("shipping_fee").over(w2).alias("shipping_fee_avg_3"),
        F.corr("shipping_fee", "amount").over(w3).alias("corr_shipping_fee_amount"),
        F.avg("discount").over(w3).alias("customer_id_avg_discount"),
        F.avg("discount").over(w4).alias("discount_avg_3_prior"),
    )
)
