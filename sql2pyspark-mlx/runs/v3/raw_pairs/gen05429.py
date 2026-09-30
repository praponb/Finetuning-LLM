from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("sale_date")
    .subtract(df_orders.filter(F.col("discount").isNotNull()).select("order_date"))
)
