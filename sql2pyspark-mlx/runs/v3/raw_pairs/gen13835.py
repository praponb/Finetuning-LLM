from pyspark.sql import functions as F

result = (
    df_shipments
    .select("shipped_date")
    .union(df_products.select("launch_date"))
    .orderBy("shipped_date")
)
