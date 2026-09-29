from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "right")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .groupBy(
        F.when(F.col("s.city").isin(["London", "Berlin"]), "tier_1").otherwise("other").alias("city_group"),
    )
    .agg(
        F.sum(F.when(F.col("p.method") == "cash", 1).otherwise(0)).alias("cash_count"),
        F.round(((100 * F.sum(F.when(F.col("p.status") == "SUCCESS", 1).otherwise(0))) / F.count("*")), 1).alias("pct_success"),
    )
    .orderBy("city_group")
)
