from pyspark.sql import functions as F

result = (
    df_stores
    .withColumn("days_since_opened_date", F.datediff(F.current_date(), "opened_date"))
    .orderBy(F.col("days_since_opened_date").desc())
)
