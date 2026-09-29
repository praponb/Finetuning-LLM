from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date") < "2024-05-01")
    .select("shipped_date")
    .intersect(df_products.select("launch_date"))
)
