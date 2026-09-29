from pyspark.sql import functions as F

valid_stores = df_stores.filter(F.col("opened_date") >= "2024-03-01")

opened_year_summary = (
    valid_stores
    .groupBy(F.year("opened_date").alias("opened_year"))
    .agg(
        F.sum(F.when(F.col("city") == "Bangkok", F.col("sqft")).otherwise(0)).alias("bangkok_sqft"),
    )
)

result = (
    opened_year_summary
    .select("opened_year", "bangkok_sqft")
    .orderBy("opened_year")
)
