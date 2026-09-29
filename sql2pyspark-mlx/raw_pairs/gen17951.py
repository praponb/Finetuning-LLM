from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").isNotNull() & F.col("product_name").like("%Chair%") & (F.col("category") == "Sports"))
    .groupBy(
        F.when(F.col("stock_qty") >= 371, "high").when(F.col("stock_qty") >= 108, "mid").otherwise("low").alias("stock_qty_bucket"),
    )
    .agg(F.concat_ws(", ", F.sort_array(F.collect_set("brand"))).alias("brand_values"))
    .orderBy(F.col("brand_values").desc(), "stock_qty_bucket")
)
