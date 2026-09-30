from pyspark.sql import functions as F

result = (
    df_customers
    .select("city")
    .intersect(df_stores.select("city"))
)
