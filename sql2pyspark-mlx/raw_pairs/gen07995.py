from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_orders
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "amount",
        "shipping_fee",
        F.sum("amount").over(w).alias("running_sum_amount"),
        F.min("amount").over(w).alias("running_min_amount"),
        F.max("amount").over(w).alias("running_max_amount"),
    )
)
