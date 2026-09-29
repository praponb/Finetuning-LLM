from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .filter((F.col("c.country") == "TH") & (F.col("a.status") != "OPEN"))
    .select(F.col("a.id"), F.col("a.customer_id"), F.col("c.city"), F.col("c.email"))
)
