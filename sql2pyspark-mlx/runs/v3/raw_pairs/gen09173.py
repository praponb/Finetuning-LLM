from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("%Trader%") & ~F.col("country").isin(["DE", "UK", "SG"]) & (F.col("rating") < 2.2))
    .select(
        "city",
        "supplier_name",
        "rating",
        F.upper("supplier_name").alias("supplier_name_upper"),
    )
)
