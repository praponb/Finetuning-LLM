from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country").isNotNull())
    .select(F.lit("customer").alias("kind"), "country")
    .union(
        df_web_events
        .filter(F.col("page").isNotNull())
        .select(F.lit("web_event").alias("kind"), "page")
    )
    .distinct()
)
