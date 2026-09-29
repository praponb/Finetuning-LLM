from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.filter(F.col("country") == "TH").alias("c"), F.col("a.customer_id") == F.col("c.id"), "left_semi")
    .filter(F.year(F.col("a.opened_date")) >= 2024)
    .select(
        F.col("a.id"),
        F.col("a.account_type"),
        F.col("a.opened_date"),
        F.col("a.status"),
    )
)
