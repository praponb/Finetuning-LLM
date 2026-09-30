from pyspark.sql import functions as F

filtered_inventory = df_inventory.filter(F.col("quantity") != 107)

quantity_bucket_summary = (
    filtered_inventory
    .groupBy(
        F.when(F.col("quantity") >= 190, "high").when(F.col("quantity") >= 64, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.sum("quantity").alias("total_quantity"))
)

result = quantity_bucket_summary
