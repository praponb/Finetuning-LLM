from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .select(F.lit("web_event").alias("source"), F.col("page").alias("label"))
    .unionByName(
        df_accounts
        .filter(F.col("account_type").isNotNull())
        .select(F.lit("account").alias("source"), F.col("account_type").alias("label"))
    )
)
