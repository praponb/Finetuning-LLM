from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("order_date", "id")

result = (
    df_orders
    .filter(F.col("discount") >= 0.19)
    .select(
        "id",
        "customer_id",
        "order_date",
        "amount",
        (F.col("amount") - F.lag("amount", 1, 0).over(w)).alias("amount_change"),
    )
)
