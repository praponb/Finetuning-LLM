from pyspark.sql import functions as F

kept_keys = (
    df_transactions
    .select("txn_type")
    .subtract(
        df_transactions
        .filter(F.col("amount") < 2600)
        .select("txn_type")
    )
)

kept_rows = (
    df_transactions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.txn_type") == F.col("k.txn_type")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy("txn_type")
    .agg(
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles"),
        F.stddev_samp("amount").alias("amount_stddev"),
        F.size(F.collect_set("txn_type")).alias("num_txn_types"),
    )
    .filter(F.col("amount_stddev") >= 0)
    .orderBy(F.col("txn_type").desc())
)
