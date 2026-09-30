from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("revenue")
    .union(df_shipments.select("cost"))
)
