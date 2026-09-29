from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("city") == "Bangkok")
    .select("segment", "signup_date")
    .subtract(
        df_customers
        .filter(F.upper(F.trim("email")) == "FRANK@COMPANY.CO")
        .select("segment", "signup_date")
    )
)
