from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") > 3.1)
    .select("country", "supplier_name")
    .unionByName(
        df_suppliers
        .filter(F.col("rating") >= 3.0)
        .select("country", "supplier_name")
    )
    .distinct()
    .orderBy("country")
)
