from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("shipping_fee", "id")

result = (
    df_orders
    .select(
        "id",
        "status",
        "shipping_fee",
        "channel",
        F.row_number().over(w).alias("pos"),
    )
    .filter(F.col("pos") <= 1)
    .select("id", "status", "shipping_fee", "channel")
    .orderBy(F.col("channel").desc())
)
