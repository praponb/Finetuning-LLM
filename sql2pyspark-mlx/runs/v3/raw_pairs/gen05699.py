from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("city").isin(["Bangkok", "Tokyo", "Berlin"]))
    .select(
        "id",
        "segment",
        "age",
        "credit_limit",
        F.weekofyear("signup_date").alias("signup_date_weekofyear"),
        F.when(F.col("is_active"), "Yes").otherwise("No").alias("is_active_label"),
    )
    .orderBy(F.col("signup_date_weekofyear").desc())
)
