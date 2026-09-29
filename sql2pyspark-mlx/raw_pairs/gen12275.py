from pyspark.sql import functions as F

filtered = (
    df_accounts
    .filter((F.col("opened_date") > F.to_date(F.lit("2023-05-31"))) & F.col("customer_id").isNotNull())
    .withColumn("balance_filled", F.coalesce("balance", F.lit(0)))
)

result = (
    filtered
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id") == F.col("b.id"), "inner")
    .select(
        "src.id",
        "src.customer_id",
        "src.balance",
        "src.status",
        F.col("b.segment"),
    )
)
