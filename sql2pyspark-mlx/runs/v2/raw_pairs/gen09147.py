from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit") > 11900)
    .select("signup_date")
    .unionByName(
        df_customers
        .filter(F.col("city") == "Chiang Mai")
        .select("signup_date")
    )
    .orderBy("signup_date")
)
