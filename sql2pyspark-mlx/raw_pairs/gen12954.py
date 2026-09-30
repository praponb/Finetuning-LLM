from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("stock_qty_tier").orderBy(F.col("total_price").desc())

result = (
    df_products
    .groupBy(
        F.when(F.col("stock_qty") >= 403, "PREMIUM").otherwise("STANDARD").alias("stock_qty_tier"),
        "category",
    )
    .agg(
        F.countDistinct("category").alias("distinct_categories"),
        F.count(F.when(F.col("category") == "Electronics", 1)).alias("electronics_rows"),
        F.percentile_approx("stock_qty", [0.25, 0.5, 0.75]).alias("stock_qty_quartiles"),
        F.count(F.when(F.col("category") == "Fashion", 1)).alias("fashion_count"),
        F.sum("price").alias("total_price"),
    )
    .filter(F.col("distinct_categories") > 1)
    .withColumn("total_price_dense_rank", F.dense_rank().over(w))
    .orderBy("electronics_rows")
)
