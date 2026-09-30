from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select("supplier_name")
    .unionByName(
        df_suppliers
        .filter(~F.col("supplier_name").like("T%"))
        .select("supplier_name")
    )
    .distinct()
)
