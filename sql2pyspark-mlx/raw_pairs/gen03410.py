from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .groupBy(
        F.when(F.col("c.country").isin(["JP", "SG"]), "core").otherwise("other").alias("country_group"),
        F.when(F.year(F.col("c.signup_date")) < 2024, "before_2024").when(F.year(F.col("c.signup_date")) == 2024, "2024").otherwise("after_2024").alias("signup_cohort"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("o.channel")))).alias("channel_values"),
        F.min(F.col("o.order_date")).alias("min_order_date"),
    )
)
