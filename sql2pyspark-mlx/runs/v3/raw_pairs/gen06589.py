from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("amount").desc())

result = (
    df_orders
    .filter(F.col("shipping_fee") >= 39)
    .select(
        "id",
        "customer_id",
        "amount",
        "discount",
        F.dense_rank().over(w).alias("rnk"),
    )
    .filter(F.col("rnk") <= 3)
    .orderBy(F.col("customer_id").desc())
)
