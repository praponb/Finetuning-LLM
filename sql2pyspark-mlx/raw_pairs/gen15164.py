from pyspark.sql import functions as F

kept_keys = (
    df_stores
    .select("region", "city")
    .subtract(
        df_stores
        .filter(F.year("opened_date") == 2025)
        .select("region", "city")
    )
)

kept_rows = (
    df_stores
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.region") == F.col("k.region")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select("city", "region", "sqft")
    .orderBy(F.col("region").asc_nulls_first(), "sqft")
)
