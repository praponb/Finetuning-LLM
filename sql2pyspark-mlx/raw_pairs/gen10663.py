from pyspark.sql import functions as F

not_flagged = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (~F.col("c.merchant").isin(["Shell", "Starbucks", "Lazada"])), "left_anti")
)

result = (
    not_flagged
    .select(
        "id",
        "account_type",
        "opened_date",
        "status",
        F.when(F.col("account_type").rlike("^(CREDIT|CHECKING)$"), "EXTENDED").otherwise("GROUP_A").alias("account_type_zone"),
    )
    .orderBy(F.col("status").desc_nulls_first())
)
