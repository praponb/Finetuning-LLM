from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") == "JP")
    .select("country")
    .unionByName(
        df_suppliers
        .filter(F.col("supplier_name").like("To%"))
        .select("country")
    )
    .distinct()
)
