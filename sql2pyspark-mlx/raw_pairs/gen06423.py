from pyspark.sql import functions as F

matched_keys = (
    df_customers
    .select("segment", "id")
    .subtract(
        df_customers
        .filter(F.col("credit_limit").between(4700, 12400))
        .select("segment", "id")
    )
)

kept_rows = (
    df_customers
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.segment") == F.col("k.segment")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select("id", "email", "country", "segment", "is_active", "credit_limit", "age")
    .orderBy(F.col("id").desc_nulls_last(), F.col("segment").desc())
)
