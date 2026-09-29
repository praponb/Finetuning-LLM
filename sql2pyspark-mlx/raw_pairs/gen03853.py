from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") == "UK")
    .select("supplier_name")
    .union(df_payments.select("method"))
    .distinct()
    .orderBy("supplier_name")
)
