from pyspark.sql import functions as F

kept_keys = (
    df_accounts
    .select("id", "status")
    .intersect(
        df_accounts
        .filter(F.col("balance") <= 45000)
        .select("id", "status")
    )
)

verified = (
    df_accounts
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy("status")
    .agg(
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.max(F.when(F.col("status") == "OPEN", F.col("balance"))).alias("open_max_balance"),
    )
    .filter(F.col("open_max_balance") >= 0)
    .orderBy(F.col("open_max_balance").desc_nulls_first())
)
