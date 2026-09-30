from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        F.when(F.col("stock_qty") >= 447, "high").when(F.col("stock_qty") >= 104, "mid").otherwise("low").alias("stock_qty_bucket"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.sum(F.when(F.col("category") == "Sports", F.col("stock_qty")).otherwise(0)).alias("sports_stock_qty"),
        F.concat_ws(", ", F.sort_array(F.collect_set("brand"))).alias("brand_values"),
    )
)
