from pyspark.sql import functions as F

result = (
    df_stores
    .filter(((F.col("region") != "South") & F.col("sqft").between(5182, 12973)) | F.col("city").isin(["Chiang Mai", "New York"]))
    .agg(
        F.min("opened_date").alias("min_opened_date"),
        (F.max("sqft") - F.min("sqft")).alias("sqft_range"),
    )
    .orderBy(F.col("sqft_range").desc())
)
