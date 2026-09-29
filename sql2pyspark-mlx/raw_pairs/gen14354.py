from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").isNotNull())
    .select("id", "supplier_name")
    .unionByName(
        df_suppliers
        .filter(F.col("city").isNotNull())
        .select("id", "supplier_name")
    )
    .orderBy("supplier_name")
)
