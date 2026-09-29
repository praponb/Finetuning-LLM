from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.filter(~F.col("city").isin(["Berlin", "Tokyo", "Singapore"])).alias("c"), F.col("a.customer_id") == F.col("c.id"), "left_semi")
    .select(F.col("a.id"), F.col("a.balance"))
    .orderBy("id")
    .limit(20)
)
