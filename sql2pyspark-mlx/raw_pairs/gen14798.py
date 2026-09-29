from pyspark.sql import functions as F

result = (
    df_accounts
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .cube(F.col("c.segment").alias("segment"), F.col("s.status").alias("status"))
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("s.balance")).alias("min_balance"),
        F.grouping_id().alias("grouping_level"),
    )
)
