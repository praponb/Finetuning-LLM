from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.sum(F.when(F.col("category") == "Electronics", F.col("price")).otherwise(0)).alias("electronics_price"),
        F.max("launch_date").alias("latest_launch_date"),
    )
    .filter(F.col("electronics_price") > 40)
)
