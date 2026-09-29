from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("region") == "South") | (F.col("region") == "West"))
    .groupBy(
        F.year("opened_date").alias("opened_year"),
        F.date_trunc("MONTH", "opened_date").alias("opened_month"),
    )
    .agg(
        F.sum(F.when(F.col("region") == "East", F.col("sqft")).otherwise(0)).alias("east_sqft"),
    )
    .filter(F.col("east_sqft") > 68)
    .orderBy(F.col("opened_year").desc(), "opened_month")
    .limit(5)
)
