from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        F.date_format("launch_date", "yyyy-MM").alias("launch_month"),
        F.when(F.col("brand").isin(["Umbrella", "Globex"]), "group_a").otherwise("other").alias("brand_group"),
    )
    .agg(F.round(F.stddev("stock_qty"), 2).alias("stddev_stock_qty"))
)
