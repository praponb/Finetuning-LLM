from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel").isin(["store", "partner", "mobile"]) & F.col("store_id").isin([7, 17, 26]))
    .select(
        "id",
        "channel",
        "customer_id",
        (F.col("shipping_fee") * F.col("discount")).alias("shipping_fee_times_discount"),
        F.initcap(F.trim("status")).alias("status_title"),
    )
    .orderBy("status_title", "id")
    .limit(10)
)
