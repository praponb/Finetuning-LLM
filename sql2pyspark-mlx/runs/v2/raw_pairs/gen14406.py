from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.month("opened_date").isin([8, 10, 12]) & F.col("city").isin(["London", "Tokyo", "Chiang Mai"]))
    .withColumn("adjusted_sqft", F.round((F.col("sqft") * 100), 2))
)
