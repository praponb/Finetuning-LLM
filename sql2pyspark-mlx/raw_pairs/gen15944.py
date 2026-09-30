from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit").between(4800, 13300) & (F.col("country").isin(["DE", "JP", "US"]) | F.col("city").isin(["Tokyo", "Berlin", "Bangkok"])))
    .select(
        "email",
        "name",
        "credit_limit",
        F.concat_ws(",", "name", "email").alias("name_email_key"),
    )
)
