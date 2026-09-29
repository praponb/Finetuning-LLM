from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("browser").isin(["Chrome", "Safari"]))
    .select("id", "customer_id", "browser")
    .unionByName(
        df_sessions
        .filter(F.col("started_at") < "2023-07-31")
        .select("id", "customer_id", "browser")
    )
    .orderBy("browser")
)
