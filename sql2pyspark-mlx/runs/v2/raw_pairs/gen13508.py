from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "left")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter(F.array_contains(F.col("p.tags"), "bestseller"))
    .groupBy(
        F.date_trunc("MONTH", F.col("s.opened_date")).alias("opened_month"),
        F.col("s.city"),
    )
    .agg(F.round(F.avg(F.col("ss.units")), 2).alias("avg_units"))
    .orderBy(F.col("avg_units").desc(), "city")
)
