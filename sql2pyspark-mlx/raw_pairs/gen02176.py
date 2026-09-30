from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.size("tags") >= 2)
    .groupBy("category")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("category") == "Fashion", 1).otherwise(0))) / F.count("*")), 1).alias("pct_fashion"),
        F.round(F.avg("stock_qty"), 2).alias("avg_stock_qty"),
    )
)
