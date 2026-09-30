from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") > 2.5)
    .select("id", "supplier_name")
    .unionByName(
        df_suppliers
        .filter(F.col("rating") <= 3.0)
        .select("id", "supplier_name")
    )
    .distinct()
)
