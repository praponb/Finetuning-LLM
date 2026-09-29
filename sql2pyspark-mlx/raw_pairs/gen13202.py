from pyspark.sql import functions as F

remaining_keys = (
    df_stores
    .select("region")
    .subtract(
        df_stores
        .filter(F.col("city") == "Berlin")
        .select("region")
    )
)

verified = (
    df_stores
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.region") == F.col("k.region")), "inner")
    .select("s.*")
)

result = verified.select("region", "opened_date", "sqft")
