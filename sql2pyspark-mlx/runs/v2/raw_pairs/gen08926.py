from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") == "TH")
    .select("id", "supplier_name", "city")
    .unionByName(
        df_suppliers
        .filter(F.upper(F.trim("supplier_name")) == "SIAM PARTS CO")
        .select("id", "supplier_name", "city")
    )
)
