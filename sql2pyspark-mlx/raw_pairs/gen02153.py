from pyspark.sql import functions as F

kept_keys = (
    df_suppliers
    .select("id", "city")
    .exceptAll(
        df_suppliers
        .filter(F.col("supplier_name").like("Si%"))
        .select("id", "city")
    )
)

kept_rows = (
    df_suppliers
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "id",
        "supplier_name",
        "city",
        "rating",
        F.when(F.col("city").rlike("^(London|Tokyo)$"), "GROUP_A").when(F.col("city").rlike("^(Bangkok|Chiang Mai)$"), "OTHER_ZONE").otherwise("GROUP_B").alias("city_zone"),
    )
    .orderBy(F.col("city_zone").desc(), "city")
)
