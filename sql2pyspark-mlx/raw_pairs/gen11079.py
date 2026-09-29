from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country").isin(["UK", "DE"]))
    .select("id", "country")
    .unionByName(
        df_suppliers
        .filter(F.col("city").isin(["Berlin", "Bangkok", "Tokyo"]))
        .select("id", "country")
    )
    .distinct()
)
