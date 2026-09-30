from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isin([11, 29]) | F.month("launch_date").isin([1, 7, 8]))
    .select("id", "launch_date")
)
