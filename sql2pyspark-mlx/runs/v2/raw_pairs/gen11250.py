from pyspark.sql import functions as F

result = (
    df_stores
    .filter(~((F.col("city") == "London") | (F.col("opened_date") <= "2023-12-28")))
    .groupBy("city", "region")
    .agg(
        (F.max("sqft") - F.min("sqft")).alias("sqft_range"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
)
