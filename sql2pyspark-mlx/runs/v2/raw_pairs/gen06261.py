from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")

result = (
    df_orders
    .select(
        "id",
        "customer_id",
        "amount",
        F.min("amount").over(w).alias("customer_id_min_amount"),
    )
)
