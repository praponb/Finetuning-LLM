from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .groupBy(F.col("p.category"))
    .agg(
        (F.max(F.col("ss.units")) - F.min(F.col("ss.units"))).alias("units_range"),
        F.round(F.avg(F.col("ss.units")), 2).alias("avg_units"),
    )
)
