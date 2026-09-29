from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty").isNull())
    .groupBy("supplier_id")
    .agg(
        F.count("price").alias("price_count"),
        F.sum("cost").alias("total_cost"),
    )
    .orderBy("supplier_id", F.col("total_cost").desc())
    .limit(5)
)
