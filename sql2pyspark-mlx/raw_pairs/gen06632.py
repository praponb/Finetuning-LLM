from pyspark.sql import functions as F

remaining_keys = (
    df_suppliers
    .select("city", "country")
    .subtract(
        df_suppliers
        .filter(F.col("rating") >= 3.7)
        .select("city", "country")
    )
)

reconciled = (
    df_suppliers
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select("city")
    .orderBy(F.col("city").desc())
)
