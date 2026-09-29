from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "right")
    .filter(F.col("o.channel").isin(["partner", "mobile", "store"]))
    .groupBy(
        F.when(F.col("c.age") >= 69, "high").when(F.col("c.age") >= 27, "mid").otherwise("low").alias("age_bucket"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("o.status")))).alias("status_values"),
    )
    .orderBy("status_values", "age_bucket")
)
