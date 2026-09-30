from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("city") == "Bangkok")
    .select("id", "name", "signup_date")
    .subtract(
        df_customers
        .filter(F.col("country") == "JP")
        .select("id", "name", "signup_date")
    )
)
