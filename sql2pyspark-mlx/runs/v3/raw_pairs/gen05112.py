from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isin([18, 21]) | (F.col("order_date") <= "2024-11-01"))
    .groupBy(
        "channel",
        F.when(F.col("status").isin(["PENDING", "RETURNED"]), "core").otherwise("other").alias("status_group"),
    )
    .agg(
        F.count("discount").alias("discount_count"),
        F.max("discount").alias("max_discount"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("cnt").desc())
)
