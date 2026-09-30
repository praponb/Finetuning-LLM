from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("method").orderBy(F.col("amount_whole").desc_nulls_last())
w_part = Window.orderBy("paid_at", "id")

base = (
    df_payments
    .filter(F.col("order_id").isNotNull())
    .select(
        "id",
        "method",
        "paid_at",
        F.floor("amount").alias("amount_whole"),
        F.coalesce("method", F.lit("DIRECT")).alias("resolved_method"),
    )
)

metrics = (
    base
    .withColumn("amount_whole_rank", F.rank().over(w_recent))
    .withColumn("next_amount_whole", F.lead("amount_whole", 1, 0).over(w_part))
)

remaining_keys = (
    metrics
    .select("id", "method")
    .intersectAll(
        df_payments
        .filter(F.col("method") != "paypal")
        .select("id", "method")
    )
)

reconciled = (
    metrics
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.method") == F.col("k.method")), "inner")
    .select("s.*")
)

valid = (
    reconciled
    .filter(F.col("method").isNotNull())
    .select(
        "id",
        "method",
        "paid_at",
        "amount_whole_rank",
        F.when(F.col("amount_whole") >= 82, "High").when(F.col("amount_whole") >= 23, "Medium").otherwise("Low").alias("amount_whole_band"),
        F.coalesce("method", F.lit("DIRECT")).alias("method_clean"),
    )
)

cleaned = (
    valid
    .filter(F.col("paid_at") < "2023-10-01")
    .select("*", F.col("paid_at").cast("date").alias("paid_at_day"))
)

filtered = (
    cleaned
    .filter(F.col("paid_at_day") >= "2024-11-07")
    .withColumn("amount_whole_rank_scaled", F.round((F.col("amount_whole_rank") * 0.9), 2))
)

report = (
    filtered
    .select("paid_at", "amount_whole_band", "method_clean", "paid_at_day")
    .orderBy(F.col("paid_at_day").desc())
)

result = (
    report
    .select("paid_at", "amount_whole_band", "method_clean")
    .orderBy(F.col("paid_at").desc_nulls_last())
)
