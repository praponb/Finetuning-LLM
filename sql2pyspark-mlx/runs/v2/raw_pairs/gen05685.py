from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("launch_date") <= "2024-06-11")
    .groupBy("brand")
    .pivot("category", ["Books", "Sports", "Home", "Electronics"])
    .agg(F.max("cost"))
    .orderBy("brand")
)
