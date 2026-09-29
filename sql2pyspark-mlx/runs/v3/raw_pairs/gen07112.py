from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("launch_date") > "2024-01-01") | F.col("stock_qty").between(126, 331))
    .groupBy("brand", F.date_format("launch_date", "yyyy-MM").alias("launch_month"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("brand") == "Acme", 1).otherwise(0))) / F.count("*")), 1).alias("pct_acme"),
        F.min("price").alias("min_price"),
    )
    .orderBy(F.col("min_price").desc())
)
