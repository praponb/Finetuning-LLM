from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country").isin(["JP", "SG"]))
    .select("supplier_name")
    .subtract(
        df_suppliers
        .filter(F.col("country").isin(["JP", "TH", "US"]))
        .select("supplier_name")
    )
)
