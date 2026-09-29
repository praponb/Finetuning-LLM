from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "left")
    .groupBy(F.col("c.country"))
    .agg(
        F.min(F.col("a.opened_date")).alias("min_opened_date"),
        F.round(((100 * F.sum(F.when(F.col("a.status") == "OPEN", 1).otherwise(0))) / F.count("*")), 1).alias("pct_open"),
    )
    .orderBy(F.col("pct_open").desc(), "country")
    .limit(10)
)
