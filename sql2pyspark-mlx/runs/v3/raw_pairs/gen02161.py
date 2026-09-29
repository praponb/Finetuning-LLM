from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("sqft") <= 11540) & (F.col("city") == "Bangkok"))
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"))
    .agg(F.count("*").alias("cnt"))
)
