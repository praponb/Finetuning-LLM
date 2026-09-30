from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isin(["West", "South"]) & (F.col("city") == "Bangkok") & F.col("sqft").isNotNull())
    .select(
        "id",
        "region",
        "opened_date",
        "store_name",
        F.lower("city").alias("city_lower"),
    )
    .orderBy(F.col("id").desc())
)
