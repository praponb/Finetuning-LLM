from pyspark.sql import functions as F

filtered_suppliers = df_suppliers.filter(F.col("city") == "Chiang Mai")

suppliers_stats = (
    filtered_suppliers
    .groupBy("city", "country")
    .agg(F.sort_array(F.collect_set("city")).alias("city_list"))
)

result = (
    suppliers_stats
    .orderBy("country", "city")
    .limit(50)
)
