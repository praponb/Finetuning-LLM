from pyspark.sql import functions as F

filtered_inventory = df_inventory.filter(F.col("store_id").isin([2, 3, 4]))

quantity_bucket_summary = (
    filtered_inventory
    .groupBy(
        F.when(F.col("quantity") >= 248, "high").when(F.col("quantity") >= 44, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.countDistinct("quantity").alias("unique_stores"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
)

result = (
    quantity_bucket_summary
    .filter(F.col("unique_stores") >= 9)
    .select("quantity_bucket", "unique_stores", "quantity_range")
    .orderBy("quantity_range", "quantity_bucket")
    .limit(20)
)
