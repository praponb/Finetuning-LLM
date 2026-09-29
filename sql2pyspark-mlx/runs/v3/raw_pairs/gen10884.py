from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "status").orderBy(F.col("shipping_fee").desc())

result = (
    df_orders
    .filter(F.col("store_id").isNotNull())
    .select(
        "id",
        "channel",
        "status",
        "shipping_fee",
        F.percent_rank().over(w).alias("shipping_fee_pct_rank"),
    )
    .orderBy("channel")
)
