from pyspark.sql import functions as F

remaining_keys = (
    df_suppliers
    .select("id")
    .subtract(
        df_suppliers
        .filter(F.col("rating") > 2.4)
        .select("id")
    )
)

kept_rows = (
    df_suppliers
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "id",
        "country",
        "city",
        "rating",
        F.when(F.col("country").rlike("^(DE|UK)$"), "OTHER_ZONE").when(F.col("country").rlike("^(TH|US)$"), "EXTENDED").otherwise("CORE").alias("country_zone"),
    )
    .orderBy(F.col("id").desc_nulls_first(), "rating")
)
