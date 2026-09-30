from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("amount").desc(), "id")

result = (
    df_orders
    .select(
        "id",
        "customer_id",
        "amount",
        "order_date",
        F.row_number().over(w).alias("rn"),
    )
    .filter(F.col("rn") <= 3)
    .select("id", "customer_id", "amount", "order_date")
)
