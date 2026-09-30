from pyspark.sql import functions as F

result = (
    df_stores
    .agg(
        F.sum("sqft").alias("total_sqft"),
        F.min("opened_date").alias("first_opened_date"),
    )
)
