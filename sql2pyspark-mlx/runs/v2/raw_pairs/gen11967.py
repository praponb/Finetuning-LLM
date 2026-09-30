from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(F.quarter("launch_date").alias("launch_quarter"), "brand")
    .agg(
        F.avg("price").alias("avg_price"),
        F.sum("cost").alias("total_cost"),
    )
    .filter(F.col("total_cost") < 3200)
    .select("launch_quarter", "brand", "avg_price", "total_cost")
)
