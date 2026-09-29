from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "right")
    .filter(F.col("s.sqft") == 4760)
    .groupBy(F.date_format(F.col("s.opened_date"), "yyyy-MM").alias("opened_month"))
    .agg(
        (F.max(F.col("o.discount")) - F.min(F.col("o.discount"))).alias("discount_range"),
    )
    .filter(F.col("discount_range") > 44)
)
