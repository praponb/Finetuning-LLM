from pyspark.sql import functions as F

remaining_keys = (
    df_customers
    .select("country")
    .intersect(
        df_customers
        .filter(F.col("credit_limit") >= 9600)
        .select("country")
    )
)

reconciled = (
    df_customers
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "id",
        "email",
        "country",
        "city",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
    )
    .orderBy(F.col("city").desc_nulls_last(), "id")
)
