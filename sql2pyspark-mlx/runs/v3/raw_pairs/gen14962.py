from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        F.date_trunc("MONTH", "last_restock").alias("last_restock_month"),
        F.when(F.col("quantity") >= 184, "high").when(F.col("quantity") >= 91, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.sum("quantity").alias("total_quantity"))
    .filter(F.col("total_quantity") > 410)
    .orderBy("quantity_bucket", "last_restock_month")
    .limit(10)
)
