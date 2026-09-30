from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("region") != "East") & (F.col("sqft") != 15238) & F.col("city").isin(["Tokyo", "Chiang Mai"]))
    .withColumn("opened_date_month_end", F.last_day("opened_date"))
    .orderBy("opened_date_month_end")
)
