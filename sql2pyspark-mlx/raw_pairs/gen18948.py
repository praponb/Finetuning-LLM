from pyspark.sql import functions as F

base = (
    df_orders
    .filter((F.col("order_date") > F.lit("2023-09-19").cast("date")) & F.col("store_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        F.col("amount").cast("int").alias("amount_int"),
    )
)

result = (
    base
    .groupBy("channel")
    .agg(
        F.sum(F.when(F.col("channel") == "partner", F.col("amount_int")).otherwise(0)).alias("partner_amount_int"),
        F.countDistinct("channel").alias("distinct_channels"),
        F.size(F.collect_set("status")).alias("num_status"),
    )
)
