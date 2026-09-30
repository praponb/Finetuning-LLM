from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").isNotNull())
    .select("email")
    .subtract(df_sessions.filter(F.col("browser").isNotNull()).select("browser"))
    .orderBy("email")
)
