from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating").between(2.6, 3.6) & F.col("country").isin(["JP", "TH", "SG"]))
    .select("city", F.upper(F.trim("supplier_name")).alias("supplier_name_normalized"))
)
