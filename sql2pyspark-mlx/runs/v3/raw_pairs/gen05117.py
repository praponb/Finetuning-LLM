from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") >= 2.3)
    .select("id", "supplier_name", "city")
    .unionByName(
        df_suppliers
        .filter(F.length("supplier_name") > 8)
        .select("id", "supplier_name", "city")
    )
    .distinct()
)
