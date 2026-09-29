from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(F.date_trunc("QUARTER", "last_restock").alias("quarter_start"), "store_id")
    .agg(
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
        F.percentile_approx("quantity", 0.9).alias("quantity_p90"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles_2"),
    )
    .orderBy(F.col("quantity_p90").desc())
)
