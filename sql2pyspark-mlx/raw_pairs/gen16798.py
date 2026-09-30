from pyspark.sql import functions as F

matched_keys = (
    df_stores
    .select("city", "id")
    .intersectAll(
        df_stores
        .filter(F.length("store_name") < 13)
        .select("city", "id")
    )
)

kept_rows = (
    df_stores
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.when(F.col("store_name").rlike("^(Old Town|Mall Central)$"), "OTHER_ZONE").when(F.col("store_name").rlike("^(Siam Flagship|Downtown Express)$"), "GROUP_A").otherwise("EXTENDED").alias("store_name_zone"),
    )
    .orderBy("city")
)
