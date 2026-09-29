from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("order_date", "id")

result = (
    df_orders
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "order_date",
        "amount",
        F.sum("amount").over(w).alias("running_amount"),
    )
)
