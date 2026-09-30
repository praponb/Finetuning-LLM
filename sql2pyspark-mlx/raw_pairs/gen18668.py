from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") < 3.8)
    .select("country", "city")
    .intersect(
        df_suppliers
        .filter(F.col("supplier_name").isNotNull())
        .select("country", "city")
    )
    .orderBy("city")
)
