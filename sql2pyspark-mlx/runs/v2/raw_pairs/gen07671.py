from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.to_date("started_at") >= "2023-08-01")
    .select("customer_id", "country")
    .unionByName(
        df_sessions
        .filter(F.col("customer_id").isin([7, 12, 20, 31]))
        .select("customer_id", "country")
    )
    .distinct()
)
