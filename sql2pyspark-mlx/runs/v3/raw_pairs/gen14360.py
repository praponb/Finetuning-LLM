from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(~((F.col("a.account_type") == "CREDIT") & (F.col("a.balance") <= 23000)))
    .select(
        F.col("a.id"),
        F.col("a.status"),
        F.col("a.account_type"),
        F.col("a.opened_date"),
        F.col("c.name"),
        F.last_day(F.col("a.opened_date")).alias("opened_date_month_end"),
    )
)
