from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("amount")

result = (
    df_orders
    .select(
        "id",
        "customer_id",
        "amount",
        "status",
        F.dense_rank().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 3)
    .select("id", "customer_id", "amount", "status")
)
