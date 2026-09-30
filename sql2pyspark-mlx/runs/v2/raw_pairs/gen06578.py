from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty") > 315)
    .groupBy("category")
    .agg(
        F.max("launch_date").alias("latest_launch_date"),
        F.sum(F.when(F.col("brand") == "Stark", 1).otherwise(0)).alias("stark_count"),
    )
    .filter(F.col("stark_count") > 68)
    .orderBy("category")
)
