from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").isNull() & (F.col("rating") < 3.4))
    .select(
        "id",
        "rating",
        "supplier_name",
        F.length("supplier_name").alias("supplier_name_length"),
        F.when(F.col("country").isin(["DE", "JP"]), "Group A").otherwise("Other").alias("country_group"),
    )
)
