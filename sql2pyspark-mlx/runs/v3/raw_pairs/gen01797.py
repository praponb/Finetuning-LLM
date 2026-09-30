from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "right")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("s.region") == "East")
    .groupBy(
        F.when(F.year(F.col("s.opened_date")) < 2024, "before_2024").when(F.year(F.col("s.opened_date")) == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
        F.col("s.city"),
    )
    .agg(
        F.count(F.col("o.shipping_fee")).alias("shipping_fee_count"),
        F.sort_array(F.collect_set(F.col("o.status"))).alias("status_list"),
    )
)
