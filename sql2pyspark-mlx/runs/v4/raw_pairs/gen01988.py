from pyspark.sql import functions as F

filtered = (
    df_inventory
    .filter(F.col("last_restock") > F.lit("2023-07-14").cast("date"))
    .select("*", F.coalesce("quantity", F.lit(0)).alias("quantity_filled"))
)

result = (
    filtered
    .withColumn("quantity_filled_tier", F.when(F.col("quantity_filled") >= 77, "HIGH").otherwise("LOW"))
    .withColumn("quarter_start", F.date_trunc("QUARTER", "last_restock"))
    .groupingSets(
        [["quantity_filled_tier", "quarter_start"], ["quantity_filled_tier"], []],
        "quantity_filled_tier",
        "quarter_start",
    )
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.sum("quantity_filled").alias("total_quantity_filled"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
    )
    .filter(F.col("total_quantity_filled") >= 0)
    .orderBy(F.col("total_quantity").desc_nulls_first())
)
