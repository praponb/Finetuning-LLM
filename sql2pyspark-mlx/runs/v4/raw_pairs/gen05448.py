from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("city") == "New York") & (F.col("region") == "Central"))
    .withColumn("opened_date_weekofyear", F.weekofyear("opened_date"))
)
