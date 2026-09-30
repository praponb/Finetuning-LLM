from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .groupBy(
        F.date_trunc("MONTH", F.col("p.launch_date")).alias("launch_month"),
        F.year(F.col("p.launch_date")).alias("launch_year"),
    )
    .agg(
        F.count(F.col("oi.unit_price")).alias("unit_price_count"),
        F.count("*").alias("cnt"),
    )
    .orderBy("launch_month")
)
