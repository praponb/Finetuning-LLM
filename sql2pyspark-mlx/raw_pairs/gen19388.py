from pyspark.sql import functions as F

customer_summary = (
    df_orders
    .filter(F.col("customer_id") == 18)
    .groupBy("customer_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("channel") == "mobile", 1).otherwise(0))) / F.count("*")), 1).alias("pct_mobile"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("t"), F.col("c.id") == F.col("t.customer_id"), "inner")
    .select(F.col("c.signup_date"), F.col("t.pct_mobile"))
    .orderBy(F.col("pct_mobile").desc())
)
