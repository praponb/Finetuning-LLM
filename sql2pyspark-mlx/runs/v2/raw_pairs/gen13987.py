from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category") != "Grocery")
    .withColumn("stock_qty_times_cost", (F.col("stock_qty") * F.col("cost")))
    .withColumn("product_name_filled", F.coalesce("product_name", F.lit("N/A")))
)
