from pyspark.sql import functions as F

matched_keys = (
    df_transactions
    .select("id")
    .intersect(
        df_transactions
        .filter(F.hour("txn_time").between(12, 16))
        .select("id")
    )
)

reconciled = (
    df_transactions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .withColumn("quarter_start", F.date_trunc("QUARTER", "txn_time"))
    .groupingSets(
        [["txn_type", "quarter_start"], ["txn_type"]],
        "txn_type",
        "quarter_start",
    )
    .agg(
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles"),
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.count("*").alias("event_count"),
        F.sum(F.when(F.col("txn_type") == "DEBIT", F.col("amount")).otherwise(0)).alias("debit_amount"),
    )
    .filter(F.col("avg_amount") >= 0)
    .orderBy(F.col("txn_type").desc())
)
