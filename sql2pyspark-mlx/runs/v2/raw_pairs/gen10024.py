from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.lower("store_name").like("%ive%"))
    .select(
        "id",
        "store_name",
        "opened_date",
        F.when(F.col("sqft") >= 17563, "High").when(F.col("sqft") >= 4958, "Medium").otherwise("Low").alias("sqft_band"),
    )
)
