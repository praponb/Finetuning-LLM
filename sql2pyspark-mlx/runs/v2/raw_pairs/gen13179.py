from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("%Paci%") & F.col("city").isin(["Chiang Mai", "Berlin", "Bangkok"]))
    .select("id", "country", "supplier_name")
    .orderBy("id")
)
