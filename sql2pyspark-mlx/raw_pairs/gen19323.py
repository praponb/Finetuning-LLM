from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        F.date_trunc("QUARTER", "last_restock").alias("quarter_start"),
        "store_id",
        "product_id",
    )
    .agg(
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.percentile_approx("quantity", 0.5).alias("quantity_p50"),
        F.count("*").alias("cnt"),
    )
)
