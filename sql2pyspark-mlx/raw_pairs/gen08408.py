from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("price") >= 740) & F.col("cost").isNotNull() & (F.col("price") >= 1070))
    .cube("category", "supplier_id")
    .agg(
        F.sum(F.when(F.col("brand") == "Umbrella", F.col("stock_qty")).otherwise(0)).alias("umbrella_stock_qty"),
        F.count("stock_qty").alias("stock_qty_count"),
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
    )
)
