from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("o.amount") >= 530)
    .groupBy(F.year(F.col("c.signup_date")).alias("signup_year"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("o.channel")))).alias("channel_values"),
        F.round(F.avg(F.col("o.discount")), 2).alias("avg_discount"),
    )
    .orderBy(F.col("avg_discount").desc())
)
