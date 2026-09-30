from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty").isNotNull() & (F.col("launch_date") <= "2025-06-22"))
    .groupBy(
        F.quarter("launch_date").alias("launch_quarter"),
        F.year("launch_date").alias("launch_year"),
    )
    .agg(
        F.sum(F.when(F.col("category") == "Electronics", F.col("cost")).otherwise(0)).alias("electronics_cost"),
    )
    .orderBy(F.col("launch_year").desc(), F.col("electronics_cost").desc())
)
