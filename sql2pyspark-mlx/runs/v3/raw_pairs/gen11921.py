from pyspark.sql import functions as F

result = (
    df_suppliers
    .select(
        "city",
        F.length("country").alias("country_length"),
        F.when(F.col("rating") >= 3.8, "High").when(F.col("rating") >= 2.0, "Medium").otherwise("Low").alias("rating_tier"),
    )
    .orderBy(F.col("city").desc())
)
