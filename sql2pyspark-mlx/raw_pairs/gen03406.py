from pyspark.sql import functions as F

kept_keys = (
    df_suppliers
    .select("city")
    .intersectAll(
        df_suppliers
        .filter(F.col("supplier_name").like("%Tokyo%"))
        .select("city")
    )
)

verified = (
    df_suppliers
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "city",
        "rating",
        F.when(F.col("city").rlike("^(London|Tokyo)$"), "CORE").when(F.col("city").rlike("^(Bangkok|Chiang Mai)$"), "EXTENDED").otherwise("GROUP_A").alias("city_zone"),
    )
    .orderBy("city_zone")
)
