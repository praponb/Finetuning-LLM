from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("segment").isin(["Corporate", "Consumer"]))
    .select("signup_date", "email")
    .intersect(
        df_customers
        .filter(F.col("credit_limit") >= 13000)
        .select("signup_date", "email")
    )
)
