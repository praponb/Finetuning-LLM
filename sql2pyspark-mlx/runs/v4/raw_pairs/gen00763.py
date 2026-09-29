from pyspark.sql import functions as F

kept_keys = (
    df_stores
    .select("id", "city")
    .intersect(
        df_stores
        .filter(F.col("region") == "North")
        .select("id", "city")
    )
)

reconciled = (
    df_stores
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        F.when(F.col("city").rlike("^(Singapore|London)$"), "EXTENDED").when(F.col("city").rlike("^(Bangkok|Chiang Mai)$"), "GROUP_B").otherwise("PRIORITY").alias("city_zone"),
    )
    .orderBy(F.col("city_zone").desc_nulls_last())
)
