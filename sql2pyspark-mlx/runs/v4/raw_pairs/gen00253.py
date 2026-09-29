from pyspark.sql import functions as F

matched_keys = (
    df_departments
    .select("location", "id")
    .exceptAll(
        df_departments
        .filter(F.col("budget").between(1710000, 2502000))
        .select("location", "id")
    )
)

reconciled = (
    df_departments
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.location") == F.col("k.location")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select("id", "location")
    .orderBy(F.col("id").desc_nulls_last())
)
