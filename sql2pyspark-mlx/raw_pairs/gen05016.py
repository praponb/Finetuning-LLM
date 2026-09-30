from pyspark.sql import functions as F

matched_keys = (
    df_suppliers
    .select("country")
    .intersect(
        df_suppliers
        .filter(F.col("rating") > 2.2)
        .select("country")
    )
)

kept_rows = (
    df_suppliers
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select("id", "supplier_name", "country", "city", "rating")
    .orderBy("city", "country")
)
