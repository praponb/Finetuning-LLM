from pyspark.sql import functions as F

not_flagged = (
    df_customers
    .alias("s")
    .join(df_accounts.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.opened_date") < F.col("s.signup_date")), "left_anti")
)

result = (
    not_flagged
    .groupBy(
        F.when(F.col("age") >= 56, "PREMIUM").otherwise("STANDARD").alias("age_tier"),
    )
    .agg(
        F.sum(F.when(F.col("city") == "Bangkok", F.col("credit_limit"))).alias("bangkok_total_credit_limit"),
        F.bit_or("age").alias("age_bit_or"),
    )
    .filter(F.col("bangkok_total_credit_limit") >= 0)
)
