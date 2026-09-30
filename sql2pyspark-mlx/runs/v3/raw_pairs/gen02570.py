from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").like("%Laptop%"))
    .groupBy("brand", F.month("launch_date").alias("launch_month"), "supplier_id")
    .agg(
        (F.max("stock_qty") - F.min("stock_qty")).alias("stock_qty_range"),
        F.count("*").alias("total_count"),
        F.count(F.when(F.col("brand") == "Globex", 1)).alias("globex_count"),
    )
    .filter(F.col("total_count") < 2)
    .orderBy("supplier_id")
)
