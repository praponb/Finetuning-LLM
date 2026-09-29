from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country", "city")
    .agg(
        F.count("*").alias("cnt"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
    .filter(F.col("cnt") < 4)
    .select("country", "city", "cnt", "city_list")
)
