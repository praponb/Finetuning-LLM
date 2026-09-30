from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id").isNull())
    .select("customer_id", "country")
    .unionByName(
        df_sessions
        .filter(F.col("country") != "US")
        .select("customer_id", "country")
    )
    .distinct()
)
