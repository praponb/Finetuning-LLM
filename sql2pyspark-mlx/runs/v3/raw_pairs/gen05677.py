from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isNotNull() & ((F.col("sqft") >= 15761) | (F.col("opened_date") <= "2024-05-01")))
    .agg(
        (F.max("sqft") - F.min("sqft")).alias("sqft_range"),
        F.min("opened_date").alias("min_opened_date"),
    )
)
