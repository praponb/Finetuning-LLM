from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("sale_date")
    .union(df_customers.select("signup_date"))
    .distinct()
)
