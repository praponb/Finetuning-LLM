from pyspark.sql import functions as F

result = (
    df_suppliers
    .select(
        "id",
        "supplier_name",
        "city",
        "country",
        F.when(F.col("country") == "DE", 1).otherwise(0).alias("is_de"),
    )
)
