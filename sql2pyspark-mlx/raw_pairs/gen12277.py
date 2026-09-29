from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city").isin(["London", "Tokyo", "Singapore"]) & (F.col("opened_date").between("2024-02-01", "2024-07-11") | (F.col("region") == "North")))
    .select(
        "city",
        "sqft",
        F.when(F.col("sqft") >= 15445, "High").when(F.col("sqft") >= 7579, "Medium").otherwise("Low").alias("sqft_band"),
    )
)
