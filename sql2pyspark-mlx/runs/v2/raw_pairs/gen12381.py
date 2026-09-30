from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.length("supplier_name") > 6)
    .select("city")
    .unionByName(df_stores.filter(F.col("opened_date") >= "2023-03-01").select("city"))
    .distinct()
    .orderBy("city")
)
