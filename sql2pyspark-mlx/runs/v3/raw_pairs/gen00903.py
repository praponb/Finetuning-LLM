from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("amount").desc())
w2 = Window.partitionBy("store_id")

result = (
    df_orders
    .select(
        "id",
        "customer_id",
        "amount",
        "store_id",
        F.rank().over(w).alias("amount_rank"),
        F.max("amount").over(w2).alias("store_id_max_amount"),
    )
    .orderBy(F.col("amount_rank").desc())
)
