from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("shipping_fee", "id")

result = (
    df_orders
    .select(
        "id",
        "customer_id",
        "shipping_fee",
        F.row_number().over(w).alias("shipping_fee_row"),
    )
)
