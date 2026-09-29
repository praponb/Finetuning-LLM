from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("account_id").orderBy("amount", "id")
w3 = Window.partitionBy("merchant").orderBy("amount", "id")
w_grp = Window.partitionBy("account_id").orderBy("amount")
w5 = Window.partitionBy("account_id").orderBy(F.col("first_known_amount").desc(), "id")
w6 = Window.partitionBy("account_id")

valid = (
    df_transactions
    .filter(F.col("txn_time") > F.to_date(F.lit("2023-04-19")))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "merchant",
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
        F.coalesce("merchant", F.lit("N/A")).alias("resolved_merchant"),
    )
)

metrics = (
    valid
    .select(
        "id",
        "account_id",
        "is_flagged_label",
        "resolved_merchant",
        F.first("amount", ignorenulls=True).over(w).alias("first_known_amount"),
        F.lead("amount", 2, 0).over(w2).alias("next_amount"),
        F.ntile(3).over(w3).alias("amount_bucket"),
        F.dense_rank().over(w_grp).alias("amount_dense_rank"),
    )
)

enriched = (
    metrics
    .select(
        "id",
        "account_id",
        "is_flagged_label",
        "resolved_merchant",
        "amount_bucket",
        F.row_number().over(w5).alias("row_seq"),
        F.round((F.col("first_known_amount") / F.sum("first_known_amount").over(w6)), 4).alias("first_known_amount_share"),
    )
)

valid_transactions = (
    enriched
    .filter(F.col("row_seq").between(7, 8))
    .select(
        "id",
        "account_id",
        "is_flagged_label",
        "resolved_merchant",
        "amount_bucket",
        "row_seq",
        "first_known_amount_share",
        F.when(F.col("row_seq") >= 8, "High").when(F.col("row_seq") >= 4, "Medium").otherwise("Low").alias("row_seq_level"),
        F.greatest("amount_bucket", "first_known_amount_share").alias("greatest_amount_bucket_first_known_amount_share"),
    )
)

result = (
    valid_transactions
    .alias("s")
    .join(df_accounts.alias("dim"), F.col("s.account_id") == F.col("dim.id"), "inner")
    .rollup(F.col("dim.status").alias("status"))
    .agg(
        F.count("*").alias("rows_total"),
        F.round(F.sum(F.col("s.greatest_amount_bucket_first_known_amount_share")), 2).alias("total_greatest_amount_bucket_first_known_amount_share"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(
        F.col("total_greatest_amount_bucket_first_known_amount_share").asc_nulls_last(),
    )
)
