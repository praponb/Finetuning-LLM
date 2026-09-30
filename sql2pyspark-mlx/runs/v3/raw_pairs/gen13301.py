from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("is_active"))
    .select("city")
    .subtract(df_stores.select("city"))
)
