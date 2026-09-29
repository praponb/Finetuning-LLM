from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.upper("product_name").like("%ACK%") & (F.col("launch_date") > "2023-10-02"))
    .groupBy("brand", "category")
    .agg(F.min("cost").alias("min_cost"))
    .filter(F.col("min_cost") >= 410)
    .orderBy(F.col("min_cost").desc(), "category")
)
