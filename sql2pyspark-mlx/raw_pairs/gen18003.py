from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .agg(
        F.count(F.when(F.col("category") == "Grocery", 1)).alias("grocery_rows"),
        F.min("price").alias("min_price"),
        F.round(F.sum("stock_qty") / F.nullif(F.count(F.when(F.col("category") == "Grocery", 1)), F.lit(0)), 2).alias("stock_qty_per_grocery"),
    )
    .filter(F.col("min_price") > 0)
    .orderBy(F.col("min_price").asc_nulls_last())
)
