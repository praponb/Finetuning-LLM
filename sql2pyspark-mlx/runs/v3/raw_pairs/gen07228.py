from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand", "supplier_id")
    .agg(
        F.count(F.when(F.col("category") == "Home", 1)).alias("home_count"),
        F.round(F.stddev("stock_qty"), 2).alias("stddev_stock_qty"),
        F.min("cost").alias("min_cost"),
    )
    .filter(F.col("min_cost") < 480)
    .drop("min_cost")
    .orderBy("brand", F.col("home_count").desc())
)
