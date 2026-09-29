from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("Pa%"))
    .select("country", "supplier_name")
    .subtract(
        df_suppliers
        .filter(F.col("country").isin(["JP", "SG", "TH"]))
        .select("country", "supplier_name")
    )
    .orderBy("country")
)
