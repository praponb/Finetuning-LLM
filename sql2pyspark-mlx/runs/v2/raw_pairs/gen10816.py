from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("device").isin(["mobile", "tablet"]))
    .select("id", "customer_id")
    .intersect(
        df_sessions
        .filter(F.col("customer_id").isNotNull())
        .select("id", "customer_id")
    )
)
