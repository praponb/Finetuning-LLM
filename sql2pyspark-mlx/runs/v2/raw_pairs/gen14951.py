from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city").isin(["London", "Chiang Mai"]) & F.col("sqft").isNotNull() & (F.col("opened_date") >= "2023-08-29"))
)
