from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("shipping_fee").desc())

result = (
    df_orders
    .filter(F.col("store_id").isin([10, 36]))
    .select(
        "id",
        "customer_id",
        "shipping_fee",
        F.rank().over(w).alias("shipping_fee_rank"),
    )
    .orderBy(F.col("shipping_fee_rank").desc(), F.col("customer_id").desc())
)
