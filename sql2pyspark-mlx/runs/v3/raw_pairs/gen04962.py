from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w2 = Window.orderBy("order_date", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_orders
    .filter(F.col("customer_id") == 33)
    .select(
        "id",
        "customer_id",
        "amount",
        "order_date",
        "shipping_fee",
        F.round((F.col("amount") / F.sum("amount").over(w)), 3).alias("amount_share"),
        F.round(F.avg("shipping_fee").over(w2), 2).alias("shipping_fee_moving_avg_7"),
    )
)
