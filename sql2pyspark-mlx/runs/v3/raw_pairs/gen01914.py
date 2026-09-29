from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") <= 1.8)
    .select("city", "supplier_name")
    .unionByName(
        df_suppliers
        .filter(F.col("city").isNotNull())
        .select("city", "supplier_name")
    )
    .orderBy("supplier_name")
)
