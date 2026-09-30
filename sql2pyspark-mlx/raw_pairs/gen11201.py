from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating").between(3.5, 4.0))
    .select("country", "city")
    .unionByName(
        df_suppliers
        .filter(F.col("supplier_name").like("%Globa%"))
        .select("country", "city")
    )
    .distinct()
    .orderBy("country")
)
