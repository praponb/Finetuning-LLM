from pyspark.sql import functions as F

matched_keys = (
    df_payments
    .select("id", "status")
    .intersect(
        df_payments
        .filter(F.to_date("paid_at") < "2023-12-01")
        .select("id", "status")
    )
)

reconciled = (
    df_payments
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .cube(F.when(F.col("amount") >= 1280, "HIGH").otherwise("LOW").alias("amount_tier"))
    .agg(
        F.size(F.collect_set("status")).alias("num_status"),
        F.sum("amount").alias("total_amount"),
    )
    .filter(F.col("total_amount") > 0)
)
