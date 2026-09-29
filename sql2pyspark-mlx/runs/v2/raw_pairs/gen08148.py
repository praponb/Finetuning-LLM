from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating").isNotNull() & F.col("country").isin(["DE", "US"])) | F.col("city").isin(["Singapore", "Berlin"]))
    .select(
        "id",
        "supplier_name",
        "rating",
        F.when(F.col("country") == "DE", 1).otherwise(0).alias("is_de"),
    )
)
