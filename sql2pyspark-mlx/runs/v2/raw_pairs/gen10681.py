from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").like("Y%") & F.col("supplier_id").isNotNull())
    .groupBy("brand")
    .agg(F.count(F.when(F.col("category") == "Fashion", 1)).alias("fashion_count"))
)
