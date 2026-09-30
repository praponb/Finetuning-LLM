from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status") == "RETURNED")
    .select("delivered_date")
    .union(df_store_sales.select("sale_date"))
    .orderBy("delivered_date")
)
