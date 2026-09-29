from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())
shipping_fee_by_customer_id = df_orders.groupBy("customer_id").agg(F.min("shipping_fee").alias("min_order_shipping_fee"))

base = (
    df_orders
    .filter(F.col("order_date") > F.lit("2023-09-19").cast("date"))
    .select(
        "id",
        "customer_id",
        "store_id",
        "status",
        "channel",
        "amount",
        F.floor("discount").alias("discount_whole"),
    )
)

latest_match = (
    base
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.unit_price"), F.col("c.quantity"))
)

matched_keys = (
    latest_match
    .select("store_id", "id")
    .exceptAll(
        df_orders
        .filter(F.col("store_id").isin([2, 11, 18]))
        .select("store_id", "id")
    )
)

reconciled = (
    latest_match
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

joined = (
    reconciled
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.segment"), F.col("b.credit_limit"), F.col("b.signup_date"))
)

result = (
    joined
    .join(shipping_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "store_id",
        "status",
        "channel",
        "amount",
        "discount_whole",
        "segment",
        F.col("min_order_shipping_fee"),
    )
)
