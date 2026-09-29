from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy(F.col("amount").desc(), "id")
w2 = Window.partitionBy("order_id").orderBy(F.col("amount").desc_nulls_last())
w3 = Window.orderBy(F.col("adjusted_amount").desc_nulls_last())

cleaned = (
    df_payments
    .filter(F.col("paid_at") > F.lit("2023-06-07 16:00:00").cast("timestamp"))
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "status",
        F.to_date("paid_at").alias("paid_at_date"),
        F.trim("status").alias("clean_status"),
    )
)

deduped = (
    cleaned
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "order_id", "method", "amount", "clean_status")
)

latest = (
    deduped
    .withColumn("rn", F.rank().over(w2))
    .filter(F.col("rn") == 1)
)

matched_keys = (
    latest
    .select("order_id", "method")
    .subtract(
        df_payments
        .filter(F.col("method").isin(["card", "promptpay"]))
        .select("order_id", "method")
    )
)

verified = (
    latest
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.method") == F.col("k.method")), "inner")
    .select("s.*")
)

filtered = (
    verified
    .filter(F.col("amount").between(690, 1380))
    .select(
        "id",
        "order_id",
        "amount",
        "clean_status",
        "rn",
        F.round((F.col("amount") * 1.1), 2).alias("adjusted_amount"),
    )
)

enriched = (
    filtered
    .select(
        "id",
        "amount",
        "adjusted_amount",
        F.dense_rank().over(w3).alias("adjusted_amount_dense_rank"),
    )
)

result = (
    enriched
    .cube(
        F.when(F.col("adjusted_amount") >= 82, "PREMIUM").otherwise("STANDARD").alias("adjusted_amount_tier"),
        F.when(F.col("adjusted_amount_dense_rank") >= 9, "PREMIUM").otherwise("STANDARD").alias("adjusted_amount_dense_rank_tier"),
    )
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.round(F.avg("adjusted_amount"), 2).alias("avg_adjusted_amount"),
        F.round(F.avg("adjusted_amount"), 2).alias("avg_adjusted_amount_2"),
    )
    .orderBy(F.col("adjusted_amount_dense_rank_tier").desc())
)
