from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand").isin(["Stark", "Acme"]) | F.col("supplier_id").isNotNull())
    .withColumn("stock_qty_filled", F.coalesce("stock_qty", F.lit(0)))
)
