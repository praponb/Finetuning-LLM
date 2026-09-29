from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.upper("supplier_name").like("%URO%"))
    .select("city", "supplier_name")
    .subtract(
        df_suppliers
        .filter(~F.col("country").isin(["JP", "UK", "TH"]))
        .select("city", "supplier_name")
    )
    .orderBy(F.col("supplier_name").desc())
)
