from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance").isNotNull() & F.col("opened_date").between("2023-05-01", "2024-08-01"))
    .select(
        "id",
        "customer_id",
        F.coalesce("balance", F.lit(0)).alias("balance_filled"),
    )
    .orderBy(F.col("id").desc())
)
