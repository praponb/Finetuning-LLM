from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty") < 190)
    .groupBy("category")
    .agg(
        F.sum(F.when(F.col("brand") == "Umbrella", 1).otherwise(0)).alias("umbrella_count"),
        F.count("cost").alias("cost_count"),
    )
    .orderBy("umbrella_count")
)
