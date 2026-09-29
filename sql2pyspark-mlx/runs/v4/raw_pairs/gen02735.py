from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").like("%Old%"))
    .groupBy(
        F.year("opened_date").alias("opened_year"),
        F.month("opened_date").alias("opened_month"),
    )
    .agg(F.sum("sqft").alias("total_sqft"))
)
