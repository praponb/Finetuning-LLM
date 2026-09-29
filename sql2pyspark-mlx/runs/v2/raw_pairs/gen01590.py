from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category", "supplier_id")
    .agg(
        F.sum("stock_qty").alias("total_stock_qty"),
        F.sum(F.when(F.col("brand") == "Umbrella", 1).otherwise(0)).alias("umbrella_count"),
    )
    .filter(F.col("total_stock_qty") < 1600)
    .orderBy(F.col("umbrella_count").desc())
)
