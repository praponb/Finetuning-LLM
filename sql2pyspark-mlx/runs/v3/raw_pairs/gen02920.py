from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating").between(2.4, 3.6))
    .select("id", "supplier_name", "country")
    .unionByName(
        df_suppliers
        .filter(F.col("rating").between(1.9, 4.1))
        .select("id", "supplier_name", "country")
    )
    .distinct()
)
