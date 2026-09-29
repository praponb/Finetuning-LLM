from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_customers.alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "left")
    .filter(F.lower(F.trim(F.col("customers.name"))) == "henry ford")
    .select(
        F.col("accounts.id"),
        F.col("accounts.opened_date"),
        F.col("accounts.status"),
        F.col("customers.city"),
        F.col("customers.age"),
        F.concat_ws(",", F.col("customers.city"), F.col("customers.name")).alias("city_name_key"),
    )
)
