from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.name").isNull() & (F.col("t.merchant") != "7-Eleven"))
    .select(
        F.col("t.id"),
        F.col("t.merchant"),
        F.col("t.is_flagged"),
        F.col("a.account_type"),
        F.col("a.opened_date"),
        F.col("c.email"),
        F.round((F.col("t.amount") * 1.15), 2).alias("adjusted_amount"),
    )
    .orderBy("opened_date", "id")
    .limit(3)
)
