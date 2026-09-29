from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") == "TH")
    .select("supplier_name", F.substring("country", 1, 4).alias("country_prefix"))
)
