from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isin(["Bangkok", "New York", "Berlin"]) & (F.col("rating") > 4.1))
    .groupBy("city")
    .agg(
        (F.max("rating") - F.min("rating")).alias("rating_range"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
    )
)
