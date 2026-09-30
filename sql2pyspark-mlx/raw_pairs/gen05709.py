from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method", "order_id").orderBy("paid_at", "id")
w2 = Window.orderBy(F.col("amount").desc(), "id")
cost_by_order_id = df_shipments.groupBy("order_id").agg(F.min("cost").alias("min_shipment_cost"))

metrics = (
    df_payments
    .select(
        "*",
        F.lead("amount").over(w).alias("next_amount"),
        F.row_number().over(w).alias("seq_no"),
        F.ntile(3).over(w2).alias("amount_bucket"),
    )
)

kept_keys = (
    metrics
    .select("status")
    .exceptAll(
        df_payments
        .filter(F.col("order_id").isNull())
        .select("status")
    )
)

verified = (
    metrics
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

matched_keys = (
    verified
    .select("method", "id")
    .subtract(
        df_payments
        .filter(F.hour("paid_at").between(12, 19))
        .select("method", "id")
    )
)

reconciled = (
    verified
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.method") == F.col("k.method")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

filtered = (
    reconciled
    .filter(~F.col("method").eqNullSafe(F.lit("cash")))
    .select(
        "id",
        "order_id",
        "amount",
        "status",
        "next_amount",
        "seq_no",
        "amount_bucket",
        F.round((F.col("amount_bucket") / F.col("amount")), 2).alias("amount_bucket_per_amount"),
        (F.col("next_amount") + F.col("amount")).alias("next_amount_plus_amount"),
        F.coalesce("method", F.lit("UNKNOWN")).alias("method_filled"),
    )
)

valid = (
    filtered
    .filter(F.col("order_id").isNotNull())
    .select(
        "*",
        F.when(F.col("next_amount_plus_amount") >= 65, "High").when(F.col("next_amount_plus_amount") >= 32, "Medium").otherwise("Low").alias("next_amount_plus_amount_level"),
    )
)

result = (
    valid
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "next_amount",
        "seq_no",
        "next_amount_plus_amount",
        "next_amount_plus_amount_level",
        F.col("min_shipment_cost"),
    )
)
