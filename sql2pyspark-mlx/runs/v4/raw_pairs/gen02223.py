from pyspark.sql import functions as F

matched_keys = (
    df_stores
    .select("city", "id")
    .exceptAll(
        df_stores
        .filter(F.col("city") == "Berlin")
        .select("city", "id")
    )
)

reconciled = (
    df_stores
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = reconciled.select("id", "store_name", "city", "region", "opened_date", "sqft")
