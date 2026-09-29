from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type", "account_id").orderBy("amount", "id")
w2 = Window.partitionBy("account_id").orderBy("amount", "id")
w3 = Window.partitionBy("account_id").orderBy("amount", "id").rowsBetween(-5, Window.currentRow)

base = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .select("*", F.lower("txn_type").alias("txn_type_lower"))
)

cleaned = (
    base
    .filter(~F.col("merchant").eqNullSafe(F.lit("7-Eleven")))
    .select("*", F.substring("txn_type", 1, 4).alias("txn_type_prefix"))
)

ranked = (
    cleaned
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "account_id", "amount", "merchant", "is_flagged", "txn_type_lower")
)

scored = (
    ranked
    .filter(F.col("account_id").isin([8, 23, 31, 40]))
    .select(
        "*",
        F.lag("amount").over(w2).alias("prev_amount"),
        F.lead("amount").over(w2).alias("next_amount"),
        F.row_number().over(w2).alias("row_seq"),
        F.avg("amount").over(w3).alias("amount_avg_5"),
    )
)

matched_keys = (
    scored
    .select("id")
    .subtract(
        df_transactions
        .filter(F.col("amount").between(1900, 3400))
        .select("id")
    )
)

kept_rows = (
    scored
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .alias("a")
    .join(df_accounts.alias("p"), F.col("a.account_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.status"), F.col("p.account_type"))
)

result = (
    enriched
    .groupBy("account_id")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.stddev_samp("amount").alias("amount_stddev"),
        F.count_if(F.col("account_type") == "SAVINGS").alias("savings_events"),
        F.size(F.collect_set("status")).alias("num_status"),
    )
    .orderBy("amount_stddev")
)
