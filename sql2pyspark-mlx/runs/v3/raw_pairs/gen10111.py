from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.upper(F.trim("product_name")) == "DESK LAMP")
    .groupBy("category")
    .agg(
        F.countDistinct("stock_qty").alias("unique_suppliers"),
        F.sum("price").alias("total_price"),
    )
    .filter(F.col("unique_suppliers") > 3)
)
