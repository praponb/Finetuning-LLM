from pyspark.sql import functions as F

remaining_keys = (
    df_customers
    .select("country")
    .intersect(
        df_customers
        .filter(F.col("segment") != "Consumer")
        .select("country")
    )
)

reconciled = (
    df_customers
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select("id", "name", "country", "city", "segment", "signup_date", "is_active")
    .orderBy(F.col("id").desc_nulls_first(), F.col("city").desc())
)
