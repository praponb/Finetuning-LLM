from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").like("%@startup.io"))
    .select("city")
    .subtract(df_stores.select("city"))
)
