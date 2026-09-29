from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("unit_price") >= 350, "PREMIUM").otherwise("STANDARD").alias("unit_price_tier"),
        "product_id",
    )
    .agg(
        F.percentile_approx("unit_price", [0.25, 0.5, 0.75]).alias("unit_price_quartiles"),
        F.sum("quantity").alias("total_quantity"),
        F.percentile_approx("unit_price", [0.25, 0.5, 0.75]).alias("unit_price_quartiles_2"),
    )
    .filter(F.col("total_quantity") > 0)
)
