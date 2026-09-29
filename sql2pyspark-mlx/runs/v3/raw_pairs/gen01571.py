from pyspark.sql import functions as F

excluded_customers = df_customers.filter(F.col("email").like("%@yahoo.com")).select("id")

result = (
    df_accounts.alias("a")
    .join(excluded_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left_anti")
    .filter(F.col("a.customer_id").isNotNull() | F.lit(excluded_customers.isEmpty()))
    .select(F.col("a.id"), F.col("a.customer_id"))
    .orderBy(F.col("customer_id").desc(), F.col("id").desc())
    .limit(1)
)
