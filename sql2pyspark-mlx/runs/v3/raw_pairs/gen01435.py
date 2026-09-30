from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") <= 5)
    .groupBy("product_id")
    .agg(
        F.count("*").alias("row_count"),
        F.sum("unit_price").alias("total_unit_price"),
    )
    .filter(F.col("row_count") < 8)
    .select("product_id", "row_count", "total_unit_price")
    .orderBy("product_id")
)
