from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("browser") == "Safari")
    .select("browser", "customer_id")
    .unionByName(
        df_sessions
        .filter(F.col("country").isin(["UK", "US"]))
        .select("browser", "customer_id")
    )
    .distinct()
)
