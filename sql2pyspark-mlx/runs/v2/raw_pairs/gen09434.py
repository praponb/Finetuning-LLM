from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("browser") == "Safari")
    .select("id", "country", "browser")
    .unionByName(
        df_sessions
        .filter(F.col("customer_id") == 5)
        .select("id", "country", "browser")
    )
)
