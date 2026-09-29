from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("To%") & F.col("rating").between(3.1, 4.0))
    .select(
        "rating",
        "supplier_name",
        "city",
        F.lower("supplier_name").alias("supplier_name_lower"),
    )
)
