from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(~F.col("c.is_active"))
    .groupBy(
        F.when(F.col("c.segment").isin(["Consumer", "Corporate"]), "priority").otherwise("other").alias("segment_group"),
    )
    .agg(
        (F.max(F.col("a.balance")) - F.min(F.col("a.balance"))).alias("balance_range"),
        F.countDistinct(F.col("a.balance")).alias("unique_statuss"),
    )
    .filter(F.col("balance_range") >= 55)
)
