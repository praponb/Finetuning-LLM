from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") < 2.0)
    .select("city", "country")
    .unionByName(
        df_suppliers
        .filter(F.col("supplier_name").like("%Tok%"))
        .select("city", "country")
    )
    .orderBy("country")
)
