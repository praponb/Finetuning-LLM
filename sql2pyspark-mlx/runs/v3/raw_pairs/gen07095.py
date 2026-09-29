from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("opened_date") > "2023-03-06") & F.col("city").isin(["New York", "London", "Bangkok"]) & (F.length("store_name") < 13))
    .agg(
        F.count("sqft").alias("sqft_count"),
        F.min("opened_date").alias("first_opened_date"),
        (F.max("sqft") - F.min("sqft")).alias("sqft_range"),
    )
    .orderBy("sqft_range")
)
