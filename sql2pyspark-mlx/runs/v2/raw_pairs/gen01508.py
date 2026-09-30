from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("browser").isNull())
    .select("id", "customer_id", "country")
    .unionByName(
        df_sessions
        .filter(F.col("browser").isNull())
        .select("id", "customer_id", "country")
    )
    .distinct()
    .orderBy("id")
)
