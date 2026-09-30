from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date").isNotNull())
    .select("opened_date", F.lit("account").alias("origin"))
    .union(
        df_inventory
        .filter(F.col("last_restock").isNotNull())
        .select("last_restock", F.lit("inventory").alias("origin"))
    )
)
