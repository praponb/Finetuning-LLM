from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.month("opened_date").isin([1, 8, 9]))
    .orderBy("region", F.col("sqft").desc())
)
