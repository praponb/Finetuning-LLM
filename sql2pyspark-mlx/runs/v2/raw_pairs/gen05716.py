from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.month("opened_date").alias("opened_month"))
    .agg(
        F.min("opened_date").alias("min_opened_date"),
        (F.max("sqft") - F.min("sqft")).alias("sqft_range"),
    )
    .orderBy("min_opened_date", F.col("sqft_range").desc())
)
