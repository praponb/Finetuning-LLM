from pyspark.sql import functions as F

result = (
    df_shipments
    .select("shipped_date")
    .subtract(df_store_sales.filter(F.col("units") > 12).select("sale_date"))
    .orderBy("shipped_date")
)
