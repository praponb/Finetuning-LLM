from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region") != "South")
    .select(
        "id",
        "region",
        "city",
        "opened_date",
        F.col("sqft").cast("string").alias("sqft_str"),
    )
    .orderBy("opened_date")
)
