from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        F.when(F.col("quantity") >= 248, "high").when(F.col("quantity") >= 89, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.max("last_restock").alias("latest_last_restock"))
    .orderBy("quantity_bucket")
)
