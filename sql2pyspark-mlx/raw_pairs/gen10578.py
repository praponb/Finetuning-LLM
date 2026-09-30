from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "customer_id").orderBy(F.col("opened_date").desc_nulls_last(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())

latest = (
    df_accounts
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "customer_id")
)

not_flagged = (
    latest
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.account_id").isin([9, 14])), "left_semi")
)

enriched = (
    not_flagged
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id").eqNullSafe(F.col("d.id")), "inner")
    .select("e.*", F.col("d.segment"), F.col("d.city"), F.col("d.name"))
)

base = (
    enriched
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "customer_id",
        "segment",
        "name",
        F.concat("segment", F.lit("/"), "name").alias("segment_name"),
        F.coalesce("city", F.lit("N/A")).alias("city_filled"),
    )
)

with_last = (
    base
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.segment_name",
        F.col("c.amount").alias("transaction_amount"),
        F.col("c.txn_type").alias("transaction_txn_type"),
    )
)

result = with_last.select("id", "transaction_amount", "transaction_txn_type")
