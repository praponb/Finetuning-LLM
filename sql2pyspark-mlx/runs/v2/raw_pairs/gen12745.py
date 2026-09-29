from pyspark.sql import functions as F

customer_summary = (
    df_orders
    .groupBy("customer_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("channel"))).alias("channel_values"),
        (F.max("shipping_fee") - F.min("shipping_fee")).alias("shipping_fee_range"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "left")
    .filter(F.col("cs.shipping_fee_range") >= 65)
    .select(
        F.col("c.id"),
        F.col("c.signup_date"),
        F.col("c.city"),
        F.col("cs.channel_values"),
        F.col("cs.shipping_fee_range"),
    )
    .orderBy("shipping_fee_range")
)
