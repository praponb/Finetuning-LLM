from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.filter(~F.col("is_active")).alias("c"), F.col("a.customer_id") == F.col("c.id"), "left_semi")
    .filter(F.col("a.balance") > 65000)
    .select(
        F.col("a.id"),
        F.col("a.customer_id"),
        F.col("a.opened_date"),
        F.col("a.balance"),
    )
)
