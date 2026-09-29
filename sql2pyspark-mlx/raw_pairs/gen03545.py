from pyspark.sql import functions as F

result = (
    df_stores
    .select("store_name")
    .intersect(df_suppliers.select("country"))
    .orderBy("store_name")
)
