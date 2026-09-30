from pyspark.sql import functions as F

result = (
    df_inventory
    .withColumn("quarter_start", F.date_trunc("quarter", "last_restock"))
    .groupingSets(
        [["product_id", "quarter_start"], ["product_id"]],
        "product_id",
        "quarter_start",
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("avg_quantity").desc_nulls_last())
)
