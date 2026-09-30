from pyspark.sql import functions as F

valid_inventory = (
    df_inventory
    .filter(F.col("quantity") > 224)
    .withColumn("days_since_last_restock", F.datediff(F.current_date(), "last_restock"))
)

summary = (
    valid_inventory
    .groupBy(
        F.when(F.col("days_since_last_restock") >= 68, "high").when(F.col("days_since_last_restock") >= 18, "mid").otherwise("low").alias("days_since_last_restock_bucket"),
    )
    .agg(
        F.min("last_restock").alias("min_last_restock"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
)

result = (
    summary
    .filter(F.col("stddev_quantity") > 73)
    .orderBy("stddev_quantity", "days_since_last_restock_bucket")
    .limit(3)
)
