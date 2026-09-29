from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("browser").isin(["Edge", "Firefox", "Safari"]))
    .select("customer_id", "country")
    .unionByName(
        df_sessions
        .filter(F.col("device") == "mobile")
        .select("customer_id", "country")
    )
    .orderBy("country")
)
