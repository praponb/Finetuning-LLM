from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty") > 364)
    .groupBy(
        F.when(F.col("stock_qty") >= 366, "high").when(F.col("stock_qty") >= 157, "mid").otherwise("low").alias("stock_qty_bucket"),
    )
    .agg(
        F.min("cost").alias("min_cost"),
        F.round(((100 * F.sum(F.when(F.col("category") == "Books", 1).otherwise(0))) / F.count("*")), 1).alias("pct_books"),
    )
    .filter(F.col("min_cost") >= 530)
    .select("stock_qty_bucket", "min_cost", "pct_books")
)
