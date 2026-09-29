from pyspark.sql import functions as F

kept_keys = (
    df_customers
    .select("id", "segment")
    .subtract(
        df_customers
        .filter(F.col("city").isin(["London", "Berlin"]))
        .select("id", "segment")
    )
)

reconciled = (
    df_customers
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.segment") == F.col("k.segment")), "inner")
    .select("s.*")
)

result = reconciled.select("id", "name", "email", "city", "signup_date", "is_active", "age")
