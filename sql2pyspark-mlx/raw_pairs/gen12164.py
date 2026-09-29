from pyspark.sql import functions as F

cleaned = (
    df_customers
    .filter((F.col("signup_date") > F.lit("2023-08-29").cast("date")) & F.col("city").isNotNull())
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "credit_limit",
        "age",
        F.when(F.col("segment").isin(["Consumer", "Corporate"]), "Tier 1").otherwise("Other").alias("segment_group"),
    )
)

result = (
    cleaned
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.customer_id") == 27), "left_semi")
)
