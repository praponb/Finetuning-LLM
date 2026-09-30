from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("E%"))
    .select("city")
    .unionByName(df_stores.select("city"))
    .orderBy("city")
)
