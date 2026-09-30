from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating").between(3.8, 3.9))
    .select("id", "city", "supplier_name")
    .unionByName(
        df_suppliers
        .filter(F.col("supplier_name").like("%Siam%"))
        .select("id", "city", "supplier_name")
    )
    .distinct()
)
