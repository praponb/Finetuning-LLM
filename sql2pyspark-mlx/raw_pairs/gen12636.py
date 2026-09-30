from pyspark.sql import functions as F

kept_keys = (
    df_customers
    .select("id", "segment")
    .intersectAll(
        df_customers
        .filter(F.col("credit_limit") >= 12300)
        .select("id", "segment")
    )
)

kept_rows = (
    df_customers
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.segment") == F.col("k.segment")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy("country")
    .agg(
        F.bit_and("age").alias("age_bit_and"),
        F.min("age").alias("min_age"),
    )
)
