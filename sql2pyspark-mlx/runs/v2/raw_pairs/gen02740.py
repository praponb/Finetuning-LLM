from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("opened_date") > "2024-06-01") & (F.col("opened_date") > "2024-01-01") & F.col("sqft").between(12396, 15004))
    .groupBy(F.month("opened_date").alias("opened_month"), "region")
    .agg(
        F.sum("sqft").alias("total_sqft"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.sum(F.when(F.col("city") == "London", F.col("sqft")).otherwise(0)).alias("london_sqft"),
    )
    .filter(F.col("london_sqft") >= 35)
    .drop("london_sqft")
)
