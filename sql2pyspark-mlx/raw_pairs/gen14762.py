from pyspark.sql import functions as F

excluded_customers = df_customers.filter(F.col("email").like("%@gmail.com")).select("id")

result = (
    df_accounts.alias("a")
    .join(excluded_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left_anti")
    .filter(F.col("a.customer_id").isNotNull() | F.lit(excluded_customers.isEmpty()))
    .filter(F.col("a.customer_id").isNull())
    .select(
        F.col("a.id"),
        F.col("a.account_type"),
        F.col("a.balance"),
        F.col("a.status"),
    )
    .orderBy(F.col("id").desc(), "status")
)
