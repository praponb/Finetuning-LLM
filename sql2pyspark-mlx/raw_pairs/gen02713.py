from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") >= 7389)
    .select(
        "id",
        "sqft",
        F.year("opened_date").alias("opened_date_year"),
        F.floor("sqft").alias("sqft_whole"),
    )
)
