from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("s.opened_date").between("2023-11-16", "2025-09-01"))
    .groupBy(F.col("s.city"))
    .agg(
        F.count(F.col("o.id")).alias("order_count"),
        F.round(((100 * F.sum(F.when(F.col("o.status") == "CANCELLED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_cancelled"),
        F.avg(F.col("o.amount")).alias("avg_amount"),
    )
    .orderBy(F.col("pct_cancelled").desc())
)
