from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("s.customer_id") == 35)
    .groupBy(
        F.quarter(F.col("c.signup_date")).alias("signup_quarter"),
        F.when(F.col("c.credit_limit") >= 13900, "high").when(F.col("c.credit_limit") >= 7800, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(
        F.sort_array(F.collect_set(F.col("s.browser"))).alias("browser_list"),
        F.sum(F.when(F.col("s.device") == "tablet", 1).otherwise(0)).alias("tablet_count"),
    )
    .orderBy("signup_quarter")
)
