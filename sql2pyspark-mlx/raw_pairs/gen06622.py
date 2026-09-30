from pyspark.sql import functions as F

result = (
    df_accounts
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "inner")
    .groupingSets(
        [["country", "segment"], ["country"]],
        F.col("d.country").alias("country"),
        F.col("d.segment").alias("segment"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.min(F.col("s.balance")).alias("min_balance"),
    )
)
