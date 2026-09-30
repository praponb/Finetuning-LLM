from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "order_id")

result = (
    df_payments
    .select(
        "id",
        "status",
        "order_id",
        "amount",
        F.max("amount").over(w).alias("status_max_amount"),
    )
)
