from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("region") == "South") & (F.col("city") == "Chiang Mai"))
    .select(
        "opened_date",
        "sqft",
        F.col("sqft").cast("string").alias("sqft_num"),
        F.round((F.col("sqft") * 1.07), 2).alias("sqft_scaled"),
    )
    .orderBy(F.col("sqft_num").desc(), F.col("sqft_scaled").desc())
)
