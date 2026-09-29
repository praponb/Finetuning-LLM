from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") != "US")
    .select("city", "supplier_name")
    .intersect(
        df_suppliers
        .filter(~F.col("supplier_name").like("%Trader%"))
        .select("city", "supplier_name")
    )
    .orderBy(F.col("city").desc())
)
