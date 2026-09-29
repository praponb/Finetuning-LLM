from pyspark.sql import functions as F

result = (
    df_customers
    .select("city")
    .intersect(df_suppliers.filter(F.length("supplier_name") >= 7).select("city"))
)
