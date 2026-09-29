from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("Li%"))
    .select("id", "country", "city")
    .unionByName(
        df_suppliers
        .filter(F.col("city") == "New York")
        .select("id", "country", "city")
    )
    .orderBy(F.col("city").desc())
)
