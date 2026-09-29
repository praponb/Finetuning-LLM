from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.alias("a"), F.col("c.id") == F.col("a.customer_id"), "left_anti")
    .filter(F.col("c.segment").isin(["SMB", "Consumer", "Corporate"]))
    .select(F.col("c.id"), F.col("c.segment"), F.col("c.age"), F.col("c.city"))
    .orderBy("id")
)
