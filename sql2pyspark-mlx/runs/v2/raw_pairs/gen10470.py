from pyspark.sql import functions as F

result = (
    df_products
    .filter(~F.col("product_name").like("Yo%") & (F.col("launch_date") <= "2025-01-12"))
    .select("id", "cost", "launch_date")
    .orderBy(F.col("cost").desc(), "id")
    .limit(3)
)
