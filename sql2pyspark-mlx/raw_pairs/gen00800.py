from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.year("order_date") >= 2023) & ((F.col("discount") > 0.23) | (F.col("status") != "COMPLETED")))
    .select(
        "id",
        "status",
        "channel",
        "amount",
        F.initcap(F.trim("channel")).alias("channel_title"),
    )
    .orderBy("amount")
)
