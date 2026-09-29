from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") < 2.5)
    .select("supplier_name", "city")
    .intersect(
        df_suppliers
        .filter(F.col("rating").between(2.4, 3.7))
        .select("supplier_name", "city")
    )
)
