from pyspark.sql import functions as F

valid = (
    df_inventory
    .filter((F.col("last_restock") >= F.lit("2023-08-29").cast("date")) & (F.col("store_id") == 4))
    .select("*", F.date_add("last_restock", 78).alias("last_restock_plus_78d"))
)

result = (
    valid
    .withColumn("quantity_tier", F.when(F.col("quantity") >= 227, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["quantity_tier", "product_id", "store_id"], ["quantity_tier", "product_id"], []],
        "quantity_tier",
        "product_id",
        "store_id",
    )
    .agg(
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.count("*").alias("row_count"),
        F.sum("quantity").alias("total_quantity"),
        F.grouping("quantity_tier").alias("gid"),
    )
    .filter(F.col("row_count") < 1000)
)
