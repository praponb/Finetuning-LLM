from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").isNotNull())
    .select("supplier_name")
    .unionByName(
        df_suppliers
        .filter(F.length("supplier_name") < 5)
        .select("supplier_name")
    )
    .distinct()
)
