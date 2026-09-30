from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("c.city"))
    .agg(
        F.max(F.col("a.opened_date")).alias("last_opened_date"),
        F.countDistinct(F.col("a.balance")).alias("unique_customers"),
    )
    .orderBy("last_opened_date", "city")
    .limit(20)
)
