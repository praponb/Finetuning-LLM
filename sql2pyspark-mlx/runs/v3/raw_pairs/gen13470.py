from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("signup_date") > "2024-04-24")
    .select("country")
    .unionByName(df_sessions.filter(F.col("browser") == "Edge").select("country"))
)
