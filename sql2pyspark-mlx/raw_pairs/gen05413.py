from pyspark.sql import functions as F

kept_keys = (
    df_suppliers
    .select("id", "country")
    .intersect(
        df_suppliers
        .filter(~F.col("city").isin(["Tokyo", "Berlin"]))
        .select("id", "country")
    )
)

kept_rows = (
    df_suppliers
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

result = kept_rows.select("country", "city", "rating")
