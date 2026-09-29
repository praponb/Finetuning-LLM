from pyspark.sql import functions as F

result = (
    df_customers
    .select("city")
    .unionByName(df_stores.filter(F.col("store_name").isNotNull()).select("city"))
    .distinct()
)
