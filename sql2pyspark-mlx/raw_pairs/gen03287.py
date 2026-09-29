from pyspark.sql import functions as F

kept_keys = (
    df_stores
    .select("city", "id")
    .exceptAll(
        df_stores
        .filter(F.col("store_name").isNotNull())
        .select("city", "id")
    )
)

verified = (
    df_stores
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.when(F.col("region").rlike("^(East|North)$"), "GROUP_A").otherwise("EXTENDED").alias("region_zone"),
    )
    .orderBy(F.col("sqft").desc_nulls_first())
)
