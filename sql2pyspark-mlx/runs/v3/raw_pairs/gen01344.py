from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.alias("a"), F.col("c.id") == F.col("a.customer_id"), "left")
    .select(
        F.col("c.credit_limit"),
        F.col("c.signup_date"),
        F.col("a.customer_id"),
        F.when(F.col("c.age") >= 67, "High").when(F.col("c.age") >= 33, "Medium").otherwise("Low").alias("age_tier"),
    )
    .orderBy("customer_id", "age_tier")
)
