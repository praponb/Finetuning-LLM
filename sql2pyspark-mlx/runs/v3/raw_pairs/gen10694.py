from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel")
w2 = Window.partitionBy("customer_id").orderBy(F.col("discount").desc(), "id")

result = (
    df_orders
    .select(
        "id",
        "channel",
        "amount",
        "customer_id",
        "discount",
        F.min("amount").over(w).alias("channel_min_amount"),
        F.row_number().over(w2).alias("discount_row"),
    )
    .orderBy(F.col("id").desc())
)
