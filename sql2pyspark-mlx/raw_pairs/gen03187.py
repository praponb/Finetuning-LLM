from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        F.date_trunc("WEEK", "launch_date").alias("week_start"),
        F.when(F.col("cost") >= 550, "HIGH").otherwise("LOW").alias("cost_tier"),
    )
    .agg(
        F.percentile_approx("cost", 0.9).alias("cost_p90"),
        F.size(F.collect_set("brand")).alias("num_brands"),
        F.min("stock_qty").alias("min_stock_qty"),
        F.bit_and("stock_qty").alias("stock_qty_bit_and"),
        F.percentile_approx("stock_qty", [0.25, 0.5, 0.75]).alias("stock_qty_quartiles"),
    )
)
