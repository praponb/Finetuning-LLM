from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").isNotNull())
    .select("city")
    .intersect(
        df_suppliers
        .filter(F.col("country").isin(["UK", "TH"]))
        .select("city")
    )
)
