from pyspark.sql import functions as F

remaining_keys = (
    df_customers
    .select("id", "city")
    .subtract(
        df_customers
        .filter(F.col("is_active"))
        .select("id", "city")
    )
)

reconciled = (
    df_customers
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "id",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
    )
    .orderBy(F.col("city").desc())
)
