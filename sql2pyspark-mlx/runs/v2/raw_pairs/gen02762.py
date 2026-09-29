from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("T%"))
    .select("city")
    .intersect(df_stores.filter(F.col("city") == "Singapore").select("city"))
    .orderBy("city")
)
