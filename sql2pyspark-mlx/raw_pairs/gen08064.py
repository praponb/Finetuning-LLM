from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "customer_id").orderBy(F.col("shipping_fee").desc())

result = (
    df_orders
    .select(
        "id",
        "store_id",
        "customer_id",
        "shipping_fee",
        F.rank().over(w).alias("shipping_fee_rank"),
    )
    .orderBy("id", F.col("store_id").desc())
)
