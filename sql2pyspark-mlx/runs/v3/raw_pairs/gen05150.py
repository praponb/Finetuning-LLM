from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id").isin([16, 21]))
    .select("id", "device", "browser")
    .unionByName(
        df_sessions
        .filter(F.col("browser") != "Edge")
        .select("id", "device", "browser")
    )
)
