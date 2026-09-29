from pyspark.sql import functions as F

result = (
    df_stores
    .filter(~F.col("region").isin(["North", "East"]))
    .select(
        "id",
        "store_name",
        "sqft",
        "opened_date",
        F.col("sqft").cast("string").alias("sqft_str"),
    )
)
