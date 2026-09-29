from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.length("supplier_name") > 5)
    .select("city")
    .intersect(df_stores.select("city"))
)
