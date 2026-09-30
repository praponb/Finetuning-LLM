from pyspark.sql import functions as F

filtered = (
    df_skus
    .filter(F.lower(F.col("sku_code")).like("%77%"))
    .select(
        "id",
        "category",
        "labels",
        "is_active",
        F.coalesce(F.col("category"), F.lit("UNKNOWN")).alias("category_or_unknown"),
    )
)

sku_rows = (
    filtered
    .filter((~F.col("is_active")) & (F.col("category_or_unknown").isin("Electronics", "Beauty")))
    .select("id")
)

sku_rows_2 = (
    df_skus
    .filter(~F.col("is_active"))
    .select("id")
)

result = (
    sku_rows
    .intersectAll(sku_rows_2)
    .orderBy(F.col("id").desc())
)
