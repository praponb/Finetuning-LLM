from pyspark.sql import functions as F

recent_products = (
    df_products
    .filter((F.col("launch_date") <= "2025-09-01") & F.array_contains("tags", "bestseller"))
)

summary = (
    recent_products
    .groupBy(
        F.when(F.col("brand").isin(["Stark", "Acme"]), "group_a").otherwise("other").alias("brand_group"),
    )
    .agg(
        F.max("stock_qty").alias("max_stock_qty"),
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
    )
)

result = (
    summary
    .filter(F.col("max_stock_qty") > 110)
    .select("brand_group", "stddev_cost")
    .orderBy(F.col("stddev_cost").desc())
)
