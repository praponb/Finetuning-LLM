from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("order_date") > "2025-10-01")
    .select(
        "store_id",
        "channel",
        "order_date",
        F.trim("channel").alias("clean_channel"),
    )
    .orderBy(F.col("store_id").desc())
)
