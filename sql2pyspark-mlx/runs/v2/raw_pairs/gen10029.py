from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .filter(F.col("a.status").isin(["CLOSED", "FROZEN"]))
    .groupBy(
        F.col("c.country"),
        F.quarter(F.col("c.signup_date")).alias("signup_quarter"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("a.account_type") == "LOAN", 1).otherwise(0))) / F.count("*")), 1).alias("pct_loan"),
        F.min(F.col("a.opened_date")).alias("min_opened_date"),
    )
    .orderBy(F.col("min_opened_date").desc(), F.col("signup_quarter").desc())
)
