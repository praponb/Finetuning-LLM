from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date") > "2024-09-01")
    .select(
        "id",
        "region",
        "store_name",
        "opened_date",
        F.when(F.col("sqft") >= 14007, "High").when(F.col("sqft") >= 4639, "Medium").otherwise("Low").alias("sqft_tier"),
    )
)
