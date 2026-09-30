from pyspark.sql import functions as F

result = (
    df_sessions
    .select(
        "id",
        "browser",
        "customer_id",
        "country",
        F.trim("device").alias("clean_device"),
        F.to_date("started_at").alias("started_at_date"),
    )
    .orderBy("id")
)
