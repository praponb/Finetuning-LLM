from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([2, 6, 10]))
    .select("sale_date")
    .subtract(df_shipments.select("delivered_date"))
)
