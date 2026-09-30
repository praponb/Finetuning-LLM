from pyspark.sql import functions as F

remaining_keys = (
    df_suppliers
    .select("country", "city")
    .subtract(
        df_suppliers
        .filter(F.col("country") == "US")
        .select("country", "city")
    )
)

kept_rows = (
    df_suppliers
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.country") == F.col("k.country")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select("id", "supplier_name", "country", "city", "rating")
    .orderBy("country")
)
