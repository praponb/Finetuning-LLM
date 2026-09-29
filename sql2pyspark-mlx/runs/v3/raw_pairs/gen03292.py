from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "right")
    .filter(F.col("a.customer_id").isNull())
    .groupBy(F.col("c.segment"))
    .agg(
        F.countDistinct(F.col("a.balance")).alias("unique_account_types"),
        F.max(F.col("a.opened_date")).alias("last_opened_date"),
    )
    .orderBy(F.col("unique_account_types").desc(), F.col("segment").desc())
    .limit(5)
)
