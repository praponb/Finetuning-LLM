from pyspark.sql import functions as F

filtered_suppliers = (
    df_suppliers
    .filter(((F.col("country") != "DE") & (F.col("rating") > 2.4)) | ~F.col("country").isin(["TH", "US"]))
)

city_summary = (
    filtered_suppliers
    .groupBy("city")
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        (F.max("rating") - F.min("rating")).alias("rating_range"),
    )
)

result = city_summary.select("city", "rating_range")
