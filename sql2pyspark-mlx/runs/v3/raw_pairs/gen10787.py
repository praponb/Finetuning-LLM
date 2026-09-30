from pyspark.sql import functions as F

customer_totals = (
    df_sessions
    .groupBy("customer_id")
    .agg(
        F.count(F.when(F.col("country") == "TH", 1)).alias("th_count"),
        F.max("started_at").alias("last_started_at"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("ct"), F.col("c.id") == F.col("ct.customer_id"), "left")
    .filter(F.col("ct.th_count") > 5)
    .select(
        F.col("c.id"),
        F.col("c.signup_date"),
        F.col("ct.th_count"),
        F.col("ct.last_started_at"),
    )
    .orderBy(F.col("id").desc(), "signup_date")
)
