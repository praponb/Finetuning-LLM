from pyspark.sql import functions as F

kept_keys = (
    df_customers
    .select("id", "country")
    .intersectAll(
        df_customers
        .filter(F.col("signup_date").between("2023-04-01", "2024-03-01"))
        .select("id", "country")
    )
)

verified = (
    df_customers
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "is_active",
        "age",
        F.when(F.col("segment").rlike("^(Consumer|Corporate)$"), "GROUP_A").when(F.col("segment").rlike("^(SMB)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("segment_zone"),
    )
    .orderBy(F.col("age").desc())
)
