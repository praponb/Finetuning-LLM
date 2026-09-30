from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("launch_date").between("2024-09-01", "2025-09-01"))
    .groupBy("brand")
    .pivot("category", ["Fashion", "Electronics"])
    .agg(F.count("stock_qty"))
)
