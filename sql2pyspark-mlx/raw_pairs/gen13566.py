from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        F.date_trunc("QUARTER", "last_restock").alias("quarter_start"),
        F.date_trunc("WEEK", "last_restock").alias("week_start"),
    )
    .agg(
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.percentile_approx("quantity", 0.95).alias("quantity_p95"),
        F.min("quantity").alias("min_quantity"),
        F.percentile_approx("quantity", 0.9).alias("quantity_p90"),
        F.bit_and("quantity").alias("quantity_bit_and_2"),
        F.min("quantity").alias("min_quantity_2"),
    )
    .orderBy("min_quantity_2")
)
