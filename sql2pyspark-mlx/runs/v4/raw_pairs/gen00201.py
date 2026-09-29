from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("city") != "Bangkok") | F.col("country").isin(["UK", "DE"]))
    .groupBy("city", "country")
    .agg(
        F.round(F.stddev("rating"), 2).alias("stddev_rating"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
    .filter(F.col("stddev_rating") >= 77)
)
