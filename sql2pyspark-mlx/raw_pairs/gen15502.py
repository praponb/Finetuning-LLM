from pyspark.sql import functions as F

matched_keys = (
    df_customers
    .select("city", "id")
    .exceptAll(
        df_customers
        .filter(F.upper(F.trim("email")) == "JACK@STARTUP.IO")
        .select("city", "id")
    )
)

reconciled = (
    df_customers
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "id",
        "country",
        "city",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.when(F.col("country").rlike("^(SG|DE)$"), "GROUP_A").when(F.col("country").rlike("^(TH|US)$"), "GROUP_B").otherwise("EXTENDED").alias("country_zone"),
    )
)
