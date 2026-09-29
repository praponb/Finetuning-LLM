from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("is_active"))
    .select(
        "signup_date",
        "email",
        F.concat("name", F.lit(", "), "segment").alias("name_segment"),
    )
    .orderBy(F.col("email").desc())
)
