from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~F.col("supplier_name").like("%Supp%") | (F.col("country") == "JP"))
    .select(
        "supplier_name",
        "city",
        F.round((F.col("rating") * 1.07), 2).alias("adjusted_rating"),
    )
    .orderBy("supplier_name", "adjusted_rating")
)
