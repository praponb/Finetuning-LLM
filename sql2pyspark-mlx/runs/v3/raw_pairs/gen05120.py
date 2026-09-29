from pyspark.sql import functions as F

product_totals = (
    df_store_sales
    .filter(F.col("units") == 12)
    .groupBy("product_id")
    .agg(
        F.count("*").alias("total_count"),
        F.max("sale_date").alias("latest_sale_date"),
    )
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("t"), F.col("p.id") == F.col("t.product_id"), "left")
    .filter(F.col("t.total_count") > 9)
    .select(
        F.col("p.stock_qty"),
        F.col("p.price"),
        F.col("t.total_count"),
        F.col("t.latest_sale_date"),
    )
    .orderBy(F.col("stock_qty").desc())
)
