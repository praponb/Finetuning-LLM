from pyspark.sql import functions as F

valid_products = (
    df_products
    .filter(F.col("product_name").like("W%") & F.col("product_name").like("Sma%"))
)

launch_month_summary = (
    valid_products
    .groupBy(
        F.date_trunc("MONTH", "launch_date").alias("launch_month"),
        F.when(F.col("brand").isin(["Umbrella", "Acme"]), "priority").otherwise("other").alias("brand_group"),
    )
    .agg(
        F.countDistinct("stock_qty").alias("unique_brands"),
        F.round(F.stddev("stock_qty"), 2).alias("stddev_stock_qty"),
    )
)

result = (
    launch_month_summary
    .orderBy(F.col("unique_brands").desc(), "launch_month", "brand_group")
    .limit(5)
)
