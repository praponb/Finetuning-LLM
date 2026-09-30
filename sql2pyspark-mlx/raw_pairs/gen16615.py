from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("end_date")
    .union(df_store_sales.select("sale_date"))
    .distinct()
)
