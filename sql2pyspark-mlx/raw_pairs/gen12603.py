from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("product_id")
    .agg(
        F.count("*").alias("num_rows"),
        F.sum("quantity").alias("total_quantity"),
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.percentile_approx("quantity", 0.9).alias("quantity_p90"),
    )
    .orderBy(F.col("quantity_bit_or").desc())
)
