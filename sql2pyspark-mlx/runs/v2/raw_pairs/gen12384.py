from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel")

result = (
    df_orders
    .filter(F.col("channel") == "web")
    .select(
        "id",
        "channel",
        "discount",
        F.max("discount").over(w).alias("channel_max_discount"),
    )
)
