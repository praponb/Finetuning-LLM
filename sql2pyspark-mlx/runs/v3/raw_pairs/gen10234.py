from pyspark.sql import functions as F

result = (
    df_customers
    .select("country")
    .subtract(
        df_sessions
        .filter(F.col("browser").isin(["Edge", "Safari", "Firefox"]))
        .select("country")
    )
    .orderBy(F.col("country").desc())
)
