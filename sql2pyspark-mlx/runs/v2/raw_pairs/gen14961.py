from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isNotNull())
    .groupBy("brand")
    .agg(
        F.min("stock_qty").alias("min_stock_qty"),
        F.max("cost").alias("max_cost"),
    )
    .filter(F.col("min_stock_qty") > 140)
    .select("brand", "min_stock_qty", "max_cost")
    .orderBy(F.col("min_stock_qty").desc())
)
