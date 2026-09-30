from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty") < 150)
    .groupBy("brand")
    .agg(
        F.sum(F.when(F.col("brand") == "Stark", F.col("stock_qty")).otherwise(0)).alias("stark_stock_qty"),
        F.round(((100 * F.sum(F.when(F.col("brand") == "Stark", 1).otherwise(0))) / F.count("*")), 1).alias("pct_stark"),
    )
)
