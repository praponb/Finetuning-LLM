from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit") >= 14900)
    .select("id", "country", "email")
    .subtract(
        df_customers
        .filter(F.col("email").like("%@GMAIL.COM"))
        .select("id", "country", "email")
    )
    .orderBy("country")
)
