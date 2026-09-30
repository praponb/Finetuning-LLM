from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "left")
    .groupBy(F.col("c.city"), F.col("c.country"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("a.status") == "CLOSED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_closed"),
        F.max(F.col("a.opened_date")).alias("latest_opened_date"),
    )
    .filter(F.col("pct_closed") > 49)
    .orderBy("country", "city")
    .limit(5)
)
