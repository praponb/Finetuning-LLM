from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isin(["Singapore", "Tokyo"]))
    .select("city")
    .unionByName(df_stores.select("city"))
    .distinct()
)
