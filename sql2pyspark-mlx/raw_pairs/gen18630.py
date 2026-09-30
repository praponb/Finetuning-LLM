from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_customers.alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "outer")
    .filter((F.col("customers.country") == "DE") & ~F.col("customers.country").isin(["UK", "TH"]))
    .select(
        F.col("accounts.id"),
        F.col("accounts.customer_id"),
        F.col("customers.country"),
    )
)
