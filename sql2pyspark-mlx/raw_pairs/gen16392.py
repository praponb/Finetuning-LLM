from pyspark.sql import functions as F

with_dims = (
    df_payments
    .alias("s")
    .join(df_orders.alias("b"), F.col("s.order_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.amount").alias("order_amount"), F.col("b.shipping_fee"))
)

valid = (
    with_dims
    .filter((F.col("paid_at") >= F.lit("2023-08-19 09:00:00").cast("timestamp")) & F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "shipping_fee",
        F.initcap(F.trim("status")).alias("status_title"),
    )
)

kept_keys = (
    valid
    .select("method", "order_id")
    .exceptAll(
        df_payments
        .filter(F.col("amount") <= 1560)
        .select("method", "order_id")
    )
)

kept_rows = (
    valid
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.method") == F.col("k.method")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

base = (
    kept_rows
    .filter(F.col("shipping_fee") < 33)
    .select(
        "*",
        (F.col("amount") - F.col("shipping_fee")).alias("amount_with_shipping"),
        F.when(F.col("method") == "cash", 1).otherwise(0).alias("is_cash"),
        F.coalesce("method", F.lit("OTHER")).alias("resolved_method"),
    )
)

base_payments = (
    base
    .filter(F.col("method") != "bank_transfer")
    .withColumn("method_group", F.when(F.col("method").isin(["cash", "promptpay"]), "Tier 1").otherwise("Other"))
    .withColumn("amount_with_shipping_scaled", F.round((F.col("amount_with_shipping") * 1.1), 2))
    .withColumn("method_clean", F.coalesce("method", F.lit("UNKNOWN")))
)

result = (
    base_payments
    .select(
        "id",
        "method",
        "amount",
        "shipping_fee",
        "amount_with_shipping",
        "is_cash",
        "resolved_method",
        "method_group",
        "method_clean",
        F.when(F.col("method").rlike("^(promptpay|paypal)$"), "EXTENDED").when(F.col("method").rlike("^(card|bank_transfer)$"), "CORE").otherwise("PRIORITY").alias("method_zone"),
    )
)
