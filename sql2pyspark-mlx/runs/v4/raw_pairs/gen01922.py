from pyspark.sql import functions as F

result = (
    df_accounts
    .select("opened_date", F.lit("account").alias("source"))
    .union(
        df_inventory
        .filter(F.col("quantity") != 136)
        .select("last_restock", F.lit("inventory").alias("source"))
    )
    .distinct()
    .orderBy(F.col("source").desc())
)
