from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("%Pacif%"))
    .select("city", "supplier_name")
    .unionByName(
        df_suppliers
        .filter(~F.col("city").isin(["New York", "Bangkok", "Berlin"]))
        .select("city", "supplier_name")
    )
    .distinct()
)
