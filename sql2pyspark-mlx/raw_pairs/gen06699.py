from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date").between("2024-08-01", "2025-04-10"))
    .select("store_id", F.coalesce("revenue", F.lit(0)).alias("revenue_filled"))
)
