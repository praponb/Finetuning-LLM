from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        "store_id",
        F.date_trunc("year", "last_restock").alias("year_start"),
        F.when(F.col("quantity") >= 222, "HIGH").otherwise("LOW").alias("quantity_tier"),
    )
    .agg(
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.percentile_approx("quantity", 0.9).alias("quantity_p90"),
    )
    .orderBy("quantity_p90")
)
