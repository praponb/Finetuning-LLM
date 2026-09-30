from pyspark.sql import functions as F

result = (
    df_stores
    .select(
        "region",
        F.datediff(F.current_date(), "opened_date").alias("days_since_opened_date"),
    )
    .orderBy(F.col("region").desc())
)
