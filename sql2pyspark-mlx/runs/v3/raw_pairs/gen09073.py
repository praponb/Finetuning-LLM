from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty") <= 339)
    .groupBy(F.month("launch_date").alias("launch_month"))
    .agg(
        F.count("cost").alias("cost_count"),
        F.sum(F.when(F.col("category") == "Sports", F.col("cost")).otherwise(0)).alias("sports_cost"),
        F.sum(F.when(F.col("brand") == "Initech", 1).otherwise(0)).alias("initech_count"),
    )
    .orderBy("launch_month")
    .limit(1)
)
