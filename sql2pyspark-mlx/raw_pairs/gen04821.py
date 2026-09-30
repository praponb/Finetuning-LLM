from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("browser") == "Chrome")
    .select("browser")
    .union(
        df_accounts
        .filter(F.col("balance").between(56000, 66000))
        .select("status")
    )
    .distinct()
    .orderBy("browser")
)
