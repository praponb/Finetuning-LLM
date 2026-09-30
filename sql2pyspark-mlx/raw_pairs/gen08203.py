from pyspark.sql import functions as F

dim_totals = (
    df_accounts
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "inner")
    .groupingSets(
        [["city", "country", "account_type"], ["city"]],
        F.col("d.city").alias("city"),
        F.col("d.country").alias("country"),
        F.col("src.account_type").alias("account_type"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.sum(F.col("src.balance")).alias("total_balance"),
    )
)

result = (
    dim_totals
    .select(
        "country",
        "account_type",
        F.when(F.col("account_type").rlike("^(CREDIT|CHECKING)$"), "EXTENDED").otherwise("GROUP_A").alias("account_type_zone"),
    )
    .orderBy(F.col("country").asc_nulls_first())
)
