from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.filter(F.col("account_type").isin(["LOAN", "CREDIT", "CHECKING"])).alias("a"), F.col("c.id") == F.col("a.customer_id"), "left_anti")
    .filter(~F.col("c.city").isin(["Singapore", "Bangkok", "Chiang Mai"]))
    .select(F.col("c.id"), F.col("c.segment"))
)
