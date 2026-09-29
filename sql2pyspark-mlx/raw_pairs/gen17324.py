from pyspark.sql import functions as F

dim_totals = (
    df_orders
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "inner")
    .groupingSets(
        [["city", "status"], ["city"], []],
        F.col("p.city").alias("city"),
        F.col("src.status").alias("status"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.min(F.col("src.amount")).alias("min_amount"),
        F.max(F.col("src.shipping_fee")).alias("max_shipping_fee"),
        F.grouping_id().alias("gid"),
    )
)

result = (
    dim_totals
    .select("city", "status", "cnt", "min_amount", "max_shipping_fee", "gid")
    .orderBy(F.col("city").asc_nulls_first())
)
