from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("s.sqft") == 13931)
    .groupBy(
        F.quarter(F.col("s.opened_date")).alias("opened_quarter"),
        F.col("s.region"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("o.channel") == "web", 1).otherwise(0))) / F.count("*")), 1).alias("pct_web"),
    )
)
