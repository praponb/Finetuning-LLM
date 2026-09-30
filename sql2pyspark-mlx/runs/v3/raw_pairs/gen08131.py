from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "right")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "inner")
    .filter(F.col("c.email").like("%@outlook.com"))
    .groupBy(F.quarter(F.col("s.opened_date")).alias("opened_quarter"))
    .agg(
        F.countDistinct(F.col("o.shipping_fee")).alias("unique_stores"),
        F.round(((100 * F.sum(F.when(F.col("o.channel") == "partner", 1).otherwise(0))) / F.count("*")), 1).alias("pct_partner"),
    )
    .filter(F.col("pct_partner") > 56)
)
