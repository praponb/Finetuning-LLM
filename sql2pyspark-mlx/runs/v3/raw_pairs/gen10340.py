from pyspark.sql import functions as F

result = (
    df_customers
    .select("city")
    .subtract(df_stores.select("city"))
    .orderBy("city")
)
