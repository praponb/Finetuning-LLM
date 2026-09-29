from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("country").isin(["JP", "US"]))
    .select("customer_id", "country")
    .unionByName(
        df_sessions
        .filter(F.col("browser").isin(["Edge", "Chrome", "Safari"]))
        .select("customer_id", "country")
    )
    .distinct()
)
