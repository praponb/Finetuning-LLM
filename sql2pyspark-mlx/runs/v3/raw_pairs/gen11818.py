from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.alias("a"), F.col("a.customer_id") == F.col("c.id"), "left")
    .filter(F.col("a.account_type") == "CHECKING")
    .select(F.col("c.age"), F.col("c.signup_date"), F.col("a.opened_date"))
    .orderBy(F.col("opened_date").desc(), "age")
)
