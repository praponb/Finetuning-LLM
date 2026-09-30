from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")

result = (
    df_payments
    .filter(F.col("amount") <= 840)
    .select(
        "id",
        "order_id",
        "amount",
        F.max("amount").over(w).alias("order_id_max_amount"),
    )
)
