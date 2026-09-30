from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.year(F.col("s.opened_date")).alias("opened_year"))
    .agg(
        (F.max(F.col("o.discount")) - F.min(F.col("o.discount"))).alias("discount_range"),
        F.count(F.when(F.col("o.channel") == "partner", 1)).alias("partner_count"),
    )
    .orderBy(F.col("opened_year").desc())
    .limit(10)
)
