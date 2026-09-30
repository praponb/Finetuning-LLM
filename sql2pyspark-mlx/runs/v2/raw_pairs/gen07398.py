from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") >= 4.0)
    .select("country")
    .unionByName(
        df_suppliers
        .filter(F.col("supplier_name").like("Glo%"))
        .select("country")
    )
)
