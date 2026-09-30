from pyspark.sql import functions as F

product_totals = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.min("sale_date").alias("first_sale_date"),
        F.min("sale_date").alias("min_sale_date"),
    )
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("P"), F.col("p.id") == F.col("P.product_id"), "left")
    .select(F.col("p.brand"), F.col("P.first_sale_date"), F.col("P.min_sale_date"))
)
