from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("signup_date") <= "2024-01-01")
    .select(F.lit("customer").alias("origin"), "country")
    .unionByName(df_sessions.select(F.lit("session").alias("origin"), "country"))
    .orderBy("origin")
)
