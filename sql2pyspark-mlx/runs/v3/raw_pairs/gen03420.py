from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~F.col("country").isin(["SG", "TH"]))
    .select("country")
    .unionByName(df_suppliers.filter(F.length("supplier_name") > 11).select("country"))
)
