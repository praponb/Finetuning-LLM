from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(F.date_trunc("QUARTER", "last_restock").alias("quarter_start"))
    .agg(
        F.percentile_approx("quantity", 0.5).alias("quantity_p50"),
        F.max("quantity").alias("max_quantity"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
        F.percentile_approx("quantity", 0.95).alias("quantity_p95"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles_2"),
    )
    .orderBy(F.col("quarter_start").asc_nulls_last())
)
