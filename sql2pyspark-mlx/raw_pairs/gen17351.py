from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        F.date_trunc("MONTH", "last_restock").alias("activity_month"),
        F.date_trunc("WEEK", "last_restock").alias("week_start"),
    )
    .agg(
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
        F.bit_or("quantity").alias("quantity_bit_or"),
    )
    .orderBy(F.col("week_start").desc())
)
