from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.filter(F.col("signup_date") >= "2024-12-11").alias("c"), F.col("a.customer_id") == F.col("c.id"), "left_semi")
    .filter(F.col("a.account_type").isin(["CHECKING", "SAVINGS"]))
    .select(F.col("a.id"), F.col("a.customer_id"), F.col("a.account_type"))
    .orderBy("id")
)
