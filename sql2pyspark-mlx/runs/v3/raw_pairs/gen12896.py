from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("stock_qty") >= 255) & (F.col("stock_qty").between(218, 348) | F.col("category").isin(["Books", "Electronics", "Grocery"])))
    .groupBy(F.year("launch_date").alias("launch_year"))
    .agg(
        F.min("stock_qty").alias("min_stock_qty"),
        F.sum(F.when(F.col("category") == "Sports", F.col("cost")).otherwise(0)).alias("sports_cost"),
    )
)
