from pyspark.sql import functions as F

not_flagged = (
    df_customers
    .alias("s")
    .join(df_sessions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.started_at") < F.col("s.signup_date")), "left_semi")
)

result = (
    not_flagged
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
    )
    .orderBy(F.col("city").desc_nulls_first())
)
