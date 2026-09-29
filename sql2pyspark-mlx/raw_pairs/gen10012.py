from pyspark.sql import functions as F

result = (
    df_shipments
    .select("shipped_date")
    .subtract(df_products.select("launch_date"))
)
