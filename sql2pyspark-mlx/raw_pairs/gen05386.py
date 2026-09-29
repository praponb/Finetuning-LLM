from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isNotNull() & F.col("supplier_name").isNotNull())
    .select(
        "id",
        "supplier_name",
        F.when(F.col("city").isin(["Berlin", "Tokyo"]), "Priority").otherwise("Other").alias("city_group"),
        F.substring("supplier_name", 1, 2).alias("supplier_name_prefix"),
    )
)
