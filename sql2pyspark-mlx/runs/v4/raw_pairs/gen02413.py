from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "channel").orderBy(F.col("shipping_fee").desc(), "id")

result = (
    df_orders
    .filter(F.col("discount") <= 0.16)
    .select(
        "id",
        "customer_id",
        "channel",
        "shipping_fee",
        F.row_number().over(w).alias("row_num"),
    )
    .orderBy(F.col("channel").desc(), F.col("id").desc())
)
