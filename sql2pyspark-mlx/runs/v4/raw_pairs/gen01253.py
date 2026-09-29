from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("amount", "id")
w2 = Window.partitionBy("customer_id", "channel").orderBy("order_date", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy("amount")
w4 = Window.partitionBy("channel").orderBy("order_date", "id")

cleaned = (
    df_orders
    .filter((F.col("order_date") > F.lit("2023-11-03").cast("date")) & F.col("channel").isNotNull() & (F.col("order_date") < "2025-08-01"))
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        F.when(F.col("discount") >= 0.43, "High").when(F.col("discount") >= 0.1, "Medium").otherwise("Low").alias("discount_band"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("resolved_channel"),
    )
)

scored = (
    cleaned
    .withColumn("amount_bucket", F.ntile(5).over(w))
    .withColumn("amount_avg_6", F.avg("amount").over(w2))
    .withColumn("amount_percent_rank", F.percent_rank().over(w3))
    .withColumn("next_amount", F.lead("amount", 2, 0).over(w4))
)

kept = (
    scored
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.product_id").isin([26, 34])), "left_anti")
)

filtered = (
    kept
    .filter(F.col("amount_avg_6") <= 1319)
    .withColumn("least_amount_avg_6_amount_percent_rank", F.least("amount_avg_6", "amount_percent_rank"))
)

base = (
    filtered
    .filter(F.col("channel").isNotNull())
    .select(
        "*",
        F.upper("channel").alias("channel_upper"),
        F.when(F.col("channel").isin(["partner", "store"]), "Priority").otherwise("Other").alias("channel_group"),
    )
)

result = (
    base
    .select(
        "id",
        "store_id",
        "least_amount_avg_6_amount_percent_rank",
        F.col("amount").alias("AMOUNT"),
    )
    .unpivot(
        ["id", "store_id"],
        ["least_amount_avg_6_amount_percent_rank", "AMOUNT"],
        "kpi",
        "metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
