from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("order_date", "id")

result = (
    df_orders
    .filter(F.col("customer_id").isNull())
    .select(
        "id",
        "status",
        "order_date",
        "amount",
        F.sum("amount").over(w).alias("running_amount"),
    )
)
