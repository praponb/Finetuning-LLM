from pyspark.sql import functions as F

matched_keys = (
    df_payments
    .select("status")
    .subtract(
        df_payments
        .filter(F.col("amount") < 1140)
        .select("status")
    )
)

kept_rows = (
    df_payments
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select("order_id", "status")
    .orderBy("status")
)
