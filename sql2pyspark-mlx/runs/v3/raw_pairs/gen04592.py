from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.length("supplier_name") > 7)
    .select("country", "supplier_name")
    .unionByName(
        df_suppliers
        .filter(F.upper("supplier_name").like("%URO%"))
        .select("country", "supplier_name")
    )
)
