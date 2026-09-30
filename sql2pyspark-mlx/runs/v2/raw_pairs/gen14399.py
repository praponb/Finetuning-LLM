from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.alias("a"), F.col("c.id") == F.col("a.customer_id"), "left")
    .filter((F.col("c.signup_date") < "2023-09-01") & (F.col("a.status") == "OPEN"))
    .select(
        F.col("c.city"),
        F.col("c.is_active"),
        F.col("a.customer_id"),
        F.col("a.account_type"),
        F.col("a.balance"),
        F.initcap(F.trim(F.col("a.account_type"))).alias("account_type_title"),
    )
    .orderBy(F.col("account_type_title").desc())
)
