from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.country") == "DE")
    .select(F.col("a.opened_date"), F.col("c.signup_date"))
    .orderBy("opened_date", "signup_date")
)
