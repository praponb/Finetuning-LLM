from pyspark.sql import functions as F

filtered_suppliers = df_suppliers.filter(F.col("rating") <= 1.9)

agg = (
    filtered_suppliers
    .groupBy("country")
    .agg(F.sort_array(F.collect_set("city")).alias("city_list"))
)

result = agg.select("country", "city_list")
