from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.count(F.when(F.col("country") == "TH", 1)).alias("th_count"),
    )
    .orderBy(F.col("th_count").desc())
)
