from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("brand") == "Globex") & F.col("category").isin(["Books", "Home", "Sports"]))
    .groupBy(F.quarter("launch_date").alias("launch_quarter"))
    .agg(
        F.sum("stock_qty").alias("total_stock_qty"),
        F.sum(F.when(F.col("brand") == "Initech", F.col("cost")).otherwise(0)).alias("initech_cost"),
        F.count(F.when(F.col("category") == "Home", 1)).alias("home_count"),
    )
    .orderBy(F.col("launch_quarter").desc())
)
