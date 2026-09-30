from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        F.when(F.col("brand").isin(["Acme", "Stark"]), "group_a").otherwise("other").alias("brand_group"),
    )
    .agg((F.max("stock_qty") - F.min("stock_qty")).alias("stock_qty_range"))
    .filter(F.col("stock_qty_range") > 36)
    .select("brand_group", "stock_qty_range")
    .orderBy("brand_group")
)
