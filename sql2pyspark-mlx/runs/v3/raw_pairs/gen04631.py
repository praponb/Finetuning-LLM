from pyspark.sql import functions as F

result = (
    df_customers
    .select("signup_date")
    .union(df_stores.select("opened_date"))
    .distinct()
)
