from pyspark.sql import functions as F

by_dimension = (
    df_orders
    .alias("a")
    .join(df_stores.alias("s"), F.col("a.store_id") == F.col("s.id"), "left")
    .groupingSets(
        [["city", "status"], ["city"], []],
        F.col("s.city").alias("city"),
        F.col("a.status").alias("status"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("a.shipping_fee")).alias("max_shipping_fee"),
        F.grouping_id().alias("grouping_level"),
    )
)

result = (
    by_dimension
    .select("city", "status", "rows_total", "max_shipping_fee", "grouping_level")
    .orderBy("status", F.col("grouping_level").desc())
)
