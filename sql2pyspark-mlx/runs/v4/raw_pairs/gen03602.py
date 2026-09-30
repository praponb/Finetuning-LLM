from pyspark.sql import functions as F

excluded_customers = df_customers.filter(F.col("age").isNotNull()).select("id")

result = (
    df_accounts.alias("a")
    .join(excluded_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left_anti")
    .filter(F.col("a.customer_id").isNotNull() | F.lit(excluded_customers.isEmpty()))
    .filter(F.col("a.account_type") == "LOAN")
    .select(F.col("a.id"), F.col("a.opened_date"))
    .orderBy(F.col("id").desc(), "opened_date")
    .limit(10)
)
