from pyspark.sql import functions as F

matched_keys = (
    df_departments
    .select("id")
    .intersectAll(
        df_departments
        .filter(F.col("budget") > 1556000)
        .select("id")
    )
)

reconciled = (
    df_departments
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select("budget")
    .orderBy(F.col("budget").desc_nulls_last())
)
