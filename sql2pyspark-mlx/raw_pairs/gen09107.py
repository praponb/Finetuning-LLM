from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .agg(
        F.countDistinct("stock_qty").alias("unique_suppliers"),
        F.round(F.avg("price"), 2).alias("avg_price"),
    )
    .filter(F.col("avg_price") < 79)
    .select("brand", "unique_suppliers", "avg_price")
)
