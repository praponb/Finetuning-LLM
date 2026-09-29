from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("amount", "id")

result = (
    df_payments
    .filter(F.col("order_id") == 31)
    .select(
        "id",
        "status",
        "amount",
        "order_id",
        F.first("order_id").over(w).alias("first_order_id"),
    )
    .orderBy(F.col("first_order_id").desc())
)
