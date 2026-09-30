from pyspark.sql import functions as F

customer_stats = (
    df_orders
    .groupBy("customer_id")
    .agg(
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.sum(F.when(F.col("channel") == "web", F.col("discount")).otherwise(0)).alias("web_discount"),
    )
)

result = (
    df_customers.alias("customers")
    .join(customer_stats.alias("customer_stats"), F.col("customers.id") == F.col("customer_stats.customer_id"), "left")
    .select(
        F.col("customers.signup_date"),
        F.col("customer_stats.channel_list"),
        F.col("customer_stats.web_discount"),
    )
    .orderBy("signup_date")
)
