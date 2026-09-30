from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("orders.id") == F.col("payments.order_id"), "inner")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("orders.store_id"), "left")
    .filter(F.col("payments.status") == "FAILED")
    .groupBy(
        F.col("stores.region"),
        F.when(F.col("stores.sqft") >= 12755, "high").when(F.col("stores.sqft") >= 4331, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.max(F.col("payments.amount")).alias("max_amount"),
        F.sort_array(F.collect_set(F.col("payments.method"))).alias("method_list"),
    )
    .orderBy("max_amount", "region")
)
