from pyspark.sql import functions as F

remaining_keys = (
    df_customers
    .select("country", "id")
    .intersectAll(
        df_customers
        .filter(F.col("signup_date") > "2024-08-01")
        .select("country", "id")
    )
)

verified = (
    df_customers
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.country") == F.col("k.country")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.when(F.col("segment").rlike("^(Consumer|SMB)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("segment_zone"),
    )
    .orderBy(F.col("country").desc())
)
