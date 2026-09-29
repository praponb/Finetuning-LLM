from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("amount").desc(), "id")

result = (
    df_orders
    .select(
        "id",
        "channel",
        "amount",
        "discount",
        F.first("discount").over(w).alias("first_discount"),
    )
    .orderBy("id")
)
