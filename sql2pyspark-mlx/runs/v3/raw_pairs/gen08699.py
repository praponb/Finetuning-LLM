from pyspark.sql import functions as F

result = (
    df_suppliers
    .select("city")
    .unionByName(df_stores.filter(F.col("store_name").like("A%")).select("city"))
)
