from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isin([5, 8]) & (F.col("stock_qty") == 116))
    .groupBy(
        "supplier_id",
        F.when(F.col("stock_qty") >= 374, "high").when(F.col("stock_qty") >= 163, "mid").otherwise("low").alias("stock_qty_bucket"),
    )
    .agg(F.sort_array(F.collect_set("category")).alias("category_list"))
)
