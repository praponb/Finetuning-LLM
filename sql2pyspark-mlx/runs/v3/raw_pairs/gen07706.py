from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        "brand",
        F.when(F.col("stock_qty") >= 415, "high").when(F.col("stock_qty") >= 85, "mid").otherwise("low").alias("stock_qty_bucket"),
    )
    .agg(F.sort_array(F.collect_set("brand")).alias("brand_list"))
    .orderBy(F.col("stock_qty_bucket").desc())
)
