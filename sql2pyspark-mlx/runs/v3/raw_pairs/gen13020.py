from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty") <= 158)
    .groupBy("supplier_id")
    .agg(
        F.sum(F.when(F.col("category") == "Fashion", F.col("stock_qty")).otherwise(0)).alias("fashion_stock_qty"),
    )
    .filter(F.col("fashion_stock_qty") >= 20)
    .select("supplier_id", "fashion_stock_qty")
)
