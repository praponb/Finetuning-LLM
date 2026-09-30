from pyspark.sql import functions as F

src = (
    df_inventory
    .filter(F.month("last_restock").isin([4, 9, 10]) & F.col("quantity").between(208, 238) & (F.col("quantity") > 193))
    .withColumn("days_since_last_restock", F.datediff(F.current_date(), "last_restock"))
)

summary = (
    src
    .groupBy(
        F.when(F.col("days_since_last_restock") >= 60, "high").when(F.col("days_since_last_restock") >= 30, "mid").otherwise("low").alias("days_since_last_restock_bucket"),
        F.when(F.col("quantity") >= 262, "high").when(F.col("quantity") >= 113, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.min("last_restock").alias("first_last_restock"))
)

result = (
    summary
    .orderBy("first_last_restock", F.col("days_since_last_restock_bucket").desc())
)
