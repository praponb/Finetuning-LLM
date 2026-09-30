from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "right")
    .filter(~(~F.col("a.status").isin(["OPEN", "CLOSED", "FROZEN"]) & (F.length(F.col("c.email")) >= 10)))
    .select(F.col("a.id"), F.col("a.customer_id"), F.col("c.city"), F.col("c.email"))
    .orderBy("id", F.col("customer_id").desc())
)
