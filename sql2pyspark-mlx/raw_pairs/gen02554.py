from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") < 3.7)
    .select("country", "supplier_name")
    .unionByName(
        df_suppliers
        .filter(F.lower("supplier_name").like("%aci%"))
        .select("country", "supplier_name")
    )
    .distinct()
)
