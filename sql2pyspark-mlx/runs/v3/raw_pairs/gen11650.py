from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("name").like("%Dia%") & F.col("country").isin(["UK", "TH"]) & F.col("city").isin(["London", "Berlin"]))
    .select(
        "signup_date",
        "email",
        (F.col("credit_limit") * F.col("age")).alias("credit_limit_times_age"),
    )
)
