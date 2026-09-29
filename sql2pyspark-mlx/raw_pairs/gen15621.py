from pyspark.sql import functions as F

kept_keys = (
    df_customers
    .select("city", "id")
    .exceptAll(
        df_customers
        .filter(F.col("credit_limit").between(9300, 13100))
        .select("city", "id")
    )
)

kept_rows = (
    df_customers
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "name",
        "email",
        "city",
        "segment",
        "signup_date",
        F.when(F.col("segment").rlike("^(Consumer|Corporate)$"), "GROUP_B").otherwise("EXTENDED").alias("segment_zone"),
    )
    .orderBy(F.col("segment_zone").desc())
)
