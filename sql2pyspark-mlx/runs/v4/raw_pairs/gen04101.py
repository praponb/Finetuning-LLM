from pyspark.sql import functions as F

matched_keys = (
    df_stores
    .select("city", "region")
    .subtract(
        df_stores
        .filter(F.col("sqft").isNull())
        .select("city", "region")
    )
)

verified = (
    df_stores
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.region") == F.col("k.region")), "inner")
    .select("s.*")
)

result = verified.select("id", "store_name", "city", "region", "sqft")
