from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") == 79)
    .select("last_restock", F.lit("inventory").alias("source"))
    .union(
        df_customers
        .filter(F.col("segment").isin(["Corporate", "Consumer", "SMB"]))
        .select("signup_date", F.lit("customer").alias("source"))
    )
    .orderBy(F.col("last_restock").desc())
)
