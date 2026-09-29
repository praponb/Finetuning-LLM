from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") <= 2.7)
    .select("id", "city", "country")
    .unionByName(
        df_suppliers
        .filter(F.col("rating") >= 2.8)
        .select("id", "city", "country")
    )
    .distinct()
)
