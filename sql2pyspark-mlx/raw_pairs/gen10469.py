from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipped_date").desc(), F.col("c.id").desc())
discount_by_store_id = df_orders.groupBy("store_id").agg(F.min("discount").alias("min_order_discount"))

filtered = (
    df_orders
    .filter((F.col("order_date") > F.lit("2023-06-24").cast("date")) & F.col("status").isin(["SHIPPED", "RETURNED"]))
    .select("*", F.length("channel").alias("channel_length"))
)

with_latest = (
    filtered
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.shipped_date"))
)

kept = (
    with_latest
    .alias("s")
    .join(df_shipments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.cost").between(64, 81)), "left_anti")
)

not_flagged = (
    kept
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.product_id").isin([14, 15, 24, 29])), "left_anti")
)

by_type = (
    not_flagged
    .groupBy("store_id", "customer_id")
    .pivot("channel", ["web", "mobile"])
    .agg(F.avg("amount").alias("value_sum"), F.count(F.lit(1)).alias("cnt"))
)

result = (
    by_type
    .join(discount_by_store_id, on="store_id", how="left")
    .select(
        "store_id",
        "customer_id",
        "web_value_sum",
        "web_cnt",
        "mobile_value_sum",
        F.col("min_order_discount"),
    )
)
