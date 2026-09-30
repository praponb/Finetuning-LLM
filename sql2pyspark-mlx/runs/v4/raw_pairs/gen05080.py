from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("order_id")
    .agg(
        F.percentile_approx("quantity", 0.9).alias("quantity_p90"),
        F.stddev_samp("quantity").alias("quantity_stddev"),
    )
    .orderBy("quantity_stddev")
)
