from pyspark.sql import functions as F

kept_keys = (
    df_suppliers
    .select("id", "city")
    .exceptAll(
        df_suppliers
        .filter(F.col("city") == "Singapore")
        .select("id", "city")
    )
)

reconciled = (
    df_suppliers
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.when(F.col("supplier_name").rlike("^(Pacific Wholesale|Tokyo Traders)$"), "EXTENDED").when(F.col("supplier_name").rlike("^(Siam Parts Co|Global Supply)$"), "OTHER_ZONE").otherwise("CORE").alias("supplier_name_zone"),
    )
)
