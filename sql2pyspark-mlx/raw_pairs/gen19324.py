from pyspark.sql import functions as F

recent_customers = (
    df_customers
    .filter(F.col("city").isin(["Tokyo", "New York"]))
    .withColumn("email_upper", F.upper("email"))
)

customers_stats = (
    recent_customers
    .groupBy(
        F.quarter("signup_date").alias("signup_quarter"),
        F.date_format("signup_date", "yyyy-MM").alias("signup_month"),
    )
    .agg(
        F.sum(F.when(F.col("segment") == "Corporate", F.col("credit_limit")).otherwise(0)).alias("corporate_credit_limit"),
    )
)

result = (
    customers_stats
    .filter(F.col("corporate_credit_limit") >= 46)
    .select("signup_quarter", "signup_month", "corporate_credit_limit")
    .orderBy(F.col("signup_quarter").desc(), "signup_month")
    .limit(20)
)
