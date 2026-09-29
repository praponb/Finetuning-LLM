from pyspark.sql import functions as F

valid = (
    df_customers
    .filter((F.col("signup_date") >= F.lit("2023-05-08").cast("date")) & F.col("city").isNotNull())
    .select(
        "id",
        "email",
        "country",
        "segment",
        "is_active",
        "credit_limit",
        "age",
        F.when(F.col("is_active"), "Yes").otherwise("No").alias("is_active_label"),
    )
)

result = (
    valid
    .groupBy("country")
    .pivot("segment", ["Corporate", "Consumer"])
    .agg(F.avg("credit_limit").alias("total"))
    .select(
        "country",
        F.col("Corporate").alias("corporate"),
        F.col("Consumer").alias("consumer"),
    )
)
