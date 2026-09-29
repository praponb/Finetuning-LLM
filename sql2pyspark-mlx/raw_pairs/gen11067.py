from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.filter(F.col("age") != 55).alias("c"), F.col("a.customer_id") == F.col("c.id"), "left_semi")
    .filter(F.col("a.balance") > 37000)
    .select(F.col("a.id"), F.col("a.customer_id"))
    .orderBy(F.col("customer_id").desc())
)
