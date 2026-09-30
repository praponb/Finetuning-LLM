from pyspark.sql import functions as F

customer_stats = (
    df_orders
    .groupBy("customer_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "RETURNED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_returned"),
        F.concat_ws(", ", F.sort_array(F.collect_set("channel"))).alias("channel_values"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "inner")
    .filter(F.col("cs.pct_returned") >= 28)
    .select(
        F.col("c.credit_limit"),
        F.col("c.signup_date"),
        F.col("cs.pct_returned"),
        F.col("cs.channel_values"),
    )
    .orderBy(F.col("credit_limit").desc())
)
