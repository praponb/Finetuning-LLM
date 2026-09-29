from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.quarter("opened_date").alias("opened_quarter"),
        F.date_format("opened_date", "yyyy-MM").alias("opened_month"),
    )
    .agg(F.count(F.when(F.col("region") == "East", 1)).alias("east_count"))
)
