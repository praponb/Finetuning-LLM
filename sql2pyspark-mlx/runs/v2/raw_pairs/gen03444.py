from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("browser") == "Edge")
    .select("id", "customer_id", "browser")
    .unionByName(
        df_sessions
        .filter(F.col("device").isin(["tablet", "mobile", "desktop"]))
        .select("id", "customer_id", "browser")
    )
    .distinct()
    .orderBy("id")
)
