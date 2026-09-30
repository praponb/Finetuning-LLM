from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("orders.id") == F.col("shipments.order_id"), "inner")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "right")
    .filter(F.col("stores.opened_date") <= "2025-03-01")
    .groupBy(
        F.col("stores.city"),
        F.when(F.year(F.col("stores.opened_date")) < 2024, "before_2024").when(F.year(F.col("stores.opened_date")) == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
    )
    .agg(
        F.sum(F.when(F.col("shipments.status") == "LOST", 1).otherwise(0)).alias("lost_count"),
    )
    .orderBy("lost_count")
)
