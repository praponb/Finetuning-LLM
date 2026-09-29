from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(F.date_trunc("WEEK", "last_restock").alias("week_start"))
    .agg(
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.percentile_approx("quantity", 0.5).alias("quantity_p50"),
        F.sum("quantity").alias("total_quantity"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
        F.bit_and("quantity").alias("quantity_bit_and"),
    )
    .filter(F.col("quantity_bit_and") < 1000)
)
