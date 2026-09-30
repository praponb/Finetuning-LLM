from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").like("La%") & F.col("product_name").like("R%") & (F.col("launch_date") <= "2023-03-01"))
    .groupBy("brand")
    .agg(
        F.sum(F.when(F.col("category") == "Electronics", 1).otherwise(0)).alias("electronics_count"),
    )
    .filter(F.col("electronics_count") > 68)
    .orderBy("brand")
)
