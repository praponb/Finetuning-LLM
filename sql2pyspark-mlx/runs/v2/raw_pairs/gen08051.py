from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") < 2.2)
    .select("id", "country", "supplier_name")
    .subtract(
        df_suppliers
        .filter(F.col("rating") >= 2.2)
        .select("id", "country", "supplier_name")
    )
    .orderBy(F.col("id").desc())
)
