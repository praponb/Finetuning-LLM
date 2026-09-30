from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") <= 1.9)
    .select("id", "supplier_name")
    .unionByName(
        df_suppliers
        .filter(F.col("city") == "London")
        .select("id", "supplier_name")
    )
    .distinct()
    .orderBy("supplier_name")
)
