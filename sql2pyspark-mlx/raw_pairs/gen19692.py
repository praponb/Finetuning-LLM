from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "store_id").orderBy(F.col("amount").desc())
w2 = Window.partitionBy("status")

result = (
    df_orders
    .filter(F.col("order_date") <= "2023-09-12")
    .select(
        "id",
        "customer_id",
        "store_id",
        "amount",
        "status",
        F.rank().over(w).alias("rnk"),
        F.count("*").over(w2).alias("status_count"),
    )
    .orderBy("customer_id", F.col("status_count").desc())
)
