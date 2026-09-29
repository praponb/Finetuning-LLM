from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("city") == "London") & (F.col("opened_date") <= "2023-05-30") & (F.col("region") == "West"))
    .groupBy("region")
    .agg(F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"))
)
