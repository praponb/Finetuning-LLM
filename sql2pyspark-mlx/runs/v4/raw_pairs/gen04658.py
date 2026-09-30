from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("paid_at").asc_nulls_last(), "id")

deduped = (
    df_payments
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

kept_keys = (
    deduped
    .select("order_id", "status")
    .subtract(
        df_payments
        .filter(F.col("status") == "REFUNDED")
        .select("order_id", "status")
    )
)

kept_rows = (
    deduped
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

valid = (
    kept_rows
    .filter(F.col("status") == "SUCCESS")
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "status",
        "rn",
        F.concat("status", F.lit(" - "), "method").alias("status_method"),
    )
)

base = (
    valid
    .filter(F.col("rn") >= 1)
    .select(
        "id",
        "order_id",
        "method",
        "rn",
        "status_method",
        F.trim("method").alias("clean_method"),
        F.coalesce("method", F.lit("UNKNOWN")).alias("method_filled"),
    )
)

filtered = (
    base
    .filter(F.col("order_id").isNotNull() & F.col("order_id").isin([25, 31]))
    .select(
        "id",
        "method",
        "status_method",
        "clean_method",
        "method_filled",
        F.substring("method", 1, 4).alias("method_prefix"),
        F.coalesce("method", F.lit("N/A")).alias("method_clean"),
    )
)

filtered_payments = (
    filtered
    .filter(F.col("method").isNotNull())
    .select(
        "method",
        "clean_method",
        "method_prefix",
        "method_clean",
        F.substring("method", 1, 4).alias("method_prefix_2"),
    )
)

cleaned = (
    filtered_payments
    .filter(F.col("method").isNotNull())
    .select(
        "clean_method",
        "method_clean",
        "method_prefix_2",
        F.length("method").alias("method_length"),
    )
)

result = (
    cleaned
    .select("clean_method", "method_clean", "method_length")
    .orderBy("method_length")
)
