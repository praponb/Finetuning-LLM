from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("discount", "id")
w2 = Window.partitionBy("customer_id")

result = (
    df_orders
    .filter(F.col("status") == "CANCELLED")
    .select(
        "id",
        "discount",
        "customer_id",
        "shipping_fee",
        F.row_number().over(w).alias("discount_row"),
        F.avg("shipping_fee").over(w2).alias("customer_id_avg_shipping_fee"),
    )
)
