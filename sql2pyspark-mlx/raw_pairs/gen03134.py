from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.upper(F.trim("supplier_name")) == "LION CITY IMPORTS")
    .select(F.lit("supplier").alias("record_type"), "city")
    .unionByName(df_stores.select(F.lit("store").alias("record_type"), "city"))
    .distinct()
    .orderBy("record_type")
)
