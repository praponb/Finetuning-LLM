from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(((F.col("rating") >= 3.6) & F.col("country").isin(["DE", "UK", "US"])) | F.col("supplier_name").like("%Supp%"))
    .select(
        "id",
        "country",
        "supplier_name",
        F.round("rating", 1).alias("rating_rounded"),
    )
    .orderBy("rating_rounded", "id")
    .limit(10)
)
