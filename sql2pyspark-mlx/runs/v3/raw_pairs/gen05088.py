from pyspark.sql import functions as F

customer_totals = (
    df_accounts
    .groupBy("customer_id")
    .agg(
        F.count(F.when(F.col("account_type") == "CHECKING", 1)).alias("checking_count"),
        F.max("opened_date").alias("latest_opened_date"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("C"), F.col("c.id") == F.col("C.customer_id"), "inner")
    .filter(F.col("C.checking_count") >= 8)
    .select(
        F.col("c.signup_date"),
        F.col("C.checking_count"),
        F.col("C.latest_opened_date"),
    )
)
