from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("sale_date")
    .subtract(df_subscriptions.select("start_date"))
)
