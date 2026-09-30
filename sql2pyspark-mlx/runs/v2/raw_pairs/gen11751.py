from pyspark.sql import functions as F

recent_suppliers = df_suppliers.filter(F.col("city") == "New York")

summary = (
    recent_suppliers
    .groupBy("city", "country")
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        (F.max("rating") - F.min("rating")).alias("rating_range"),
    )
)

result = (
    summary
    .select("city", "country", "city_list", "rating_range")
    .orderBy("country")
)
