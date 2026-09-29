from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("shipping_fee")

result = (
    df_orders
    .filter(F.col("discount") >= 0.12)
    .select(
        "id",
        "customer_id",
        "shipping_fee",
        F.dense_rank().over(w).alias("shipping_fee_dense_rank"),
    )
)
