from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .filter(F.col("a.customer_id").isNotNull())
    .join(df_customers.filter(F.upper(F.trim("email")) == "ALICE@GMAIL.COM").alias("c"), F.col("a.customer_id") == F.col("c.id"), "left_anti")
    .select(F.col("a.id"), F.col("a.customer_id"))
    .orderBy(F.col("id").desc())
)
