from pyspark.sql import functions as F

kept = (
    df_products
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.revenue") >= 3300), "left_semi")
)

result = (
    kept
    .withColumn("cost_tier", F.when(F.col("cost") >= 630, "LARGE").otherwise("SMALL"))
    .rollup("cost_tier", "category", "supplier_id")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("category") == "Grocery", 1)).alias("grocery_rows"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.grouping("cost_tier").alias("grouping_id"),
    )
    .filter(F.col("distinct_ids_2") < 1)
)
