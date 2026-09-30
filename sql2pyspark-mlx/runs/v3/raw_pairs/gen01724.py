from pyspark.sql import functions as F

result = (
    df_customers
    .select("city")
    .unionByName(df_stores.filter(F.length("store_name") > 8).select("city"))
    .orderBy("city")
)
