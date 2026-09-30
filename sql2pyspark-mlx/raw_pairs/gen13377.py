from pyspark.sql import functions as F

valid = (
    df_suppliers
    .filter(~F.col("city").eqNullSafe(F.lit("New York")))
    .select(
        "id",
        "supplier_name",
        "country",
        "rating",
        F.when(F.col("rating") >= 4.1, "High").when(F.col("rating") >= 1.5, "Medium").otherwise("Low").alias("rating_tier"),
        F.substring("city", 1, 2).alias("city_prefix"),
    )
)

result = (
    valid
    .select("country")
    .exceptAll(
        df_suppliers
        .filter(F.col("rating").isNotNull())
        .select("country")
    )
)
