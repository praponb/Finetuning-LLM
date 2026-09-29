from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.date_format("opened_date", "yyyy-MM").alias("opened_month"),
        F.quarter("opened_date").alias("opened_quarter"),
    )
    .agg(
        F.min("sqft").alias("min_sqft"),
        F.max("sqft").alias("max_sqft"),
    )
)
