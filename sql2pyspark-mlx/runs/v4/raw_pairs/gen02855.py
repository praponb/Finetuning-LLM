from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("country") == "JP") & (F.col("city") == "Singapore"))
    .select(
        "supplier_name",
        F.when(F.col("rating") >= 4.4, "High").when(F.col("rating") >= 1.9, "Medium").otherwise("Low").alias("rating_tier"),
        F.length("supplier_name").alias("supplier_name_length"),
    )
)
