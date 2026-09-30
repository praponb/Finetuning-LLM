from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter(F.col("ss.revenue") <= 1900)
    .groupBy(F.year(F.col("p.launch_date")).alias("launch_year"))
    .agg(
        (F.max(F.col("ss.units")) - F.min(F.col("ss.units"))).alias("units_range"),
        F.sum(F.col("ss.units")).alias("total_units"),
    )
    .filter(F.col("total_units") > 160)
    .orderBy("total_units")
)
