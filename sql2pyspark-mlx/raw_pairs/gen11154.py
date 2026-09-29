from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("store_id", "customer_id").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_orders
    .filter((F.col("channel").eqNullSafe(F.lit("store"))) & (F.col("order_date") > F.lit("2023-04-26").cast("date")))
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "channel",
        "amount",
        "shipping_fee",
        F.when(F.col("channel").isin(["mobile", "store"]), "Group A").otherwise("Other").alias("channel_group"),
    )
)

result = (
    filtered
    .select(
        "id",
        "customer_id",
        "store_id",
        "shipping_fee",
        "channel_group",
        F.sum("amount").over(w_part).alias("running_sum_amount"),
        F.count("amount").over(w_part).alias("running_count_amount"),
        F.max("amount").over(w_part).alias("running_max_amount"),
    )
)
