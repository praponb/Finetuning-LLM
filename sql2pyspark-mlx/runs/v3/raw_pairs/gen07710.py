from pyspark.sql import functions as F

excluded_customers = df_customers.filter(F.col("segment").isNotNull()).select("id")

result = (
    df_accounts.alias("a")
    .join(excluded_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left_anti")
    .filter(F.col("a.customer_id").isNotNull() | F.lit(excluded_customers.isEmpty()))
    .filter(F.col("a.account_type").isin(["LOAN", "CHECKING", "CREDIT"]))
    .select(F.col("a.id"), F.col("a.balance"))
    .orderBy(F.col("id").desc())
)
