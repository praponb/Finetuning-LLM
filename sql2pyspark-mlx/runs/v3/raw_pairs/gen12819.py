from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.year("opened_date") == 2024)
    .select(
        "city",
        "sqft",
        "store_name",
        F.col("sqft").cast("double").alias("sqft_str"),
    )
    .orderBy("sqft")
)
