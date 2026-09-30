from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("%Tokyo%") & (F.col("country") == "JP"))
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        F.substring("supplier_name", 1, 2).alias("supplier_name_prefix"),
        F.round((F.col("rating") * 1.07), 2).alias("adjusted_rating"),
    )
)
