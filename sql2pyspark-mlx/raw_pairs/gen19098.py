from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("week_start").orderBy(F.col("fee_events").desc())

kept_keys = (
    df_transactions
    .select("id", "txn_type")
    .intersect(
        df_transactions
        .filter(F.col("amount").isNotNull())
        .select("id", "txn_type")
    )
)

verified = (
    df_transactions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.txn_type") == F.col("k.txn_type")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        F.date_trunc("WEEK", "txn_time").alias("week_start"),
        F.when(F.col("amount") >= 3000, "HIGH").otherwise("LOW").alias("amount_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.count_if(F.col("txn_type") == "DEBIT").alias("debit_events"),
        F.count_if(F.col("txn_type") == "FEE").alias("fee_events"),
        F.min("txn_time").alias("started_at"),
    )
    .withColumn("fee_events_dense_rank", F.dense_rank().over(w))
    .orderBy(F.col("row_count").desc_nulls_first())
)
