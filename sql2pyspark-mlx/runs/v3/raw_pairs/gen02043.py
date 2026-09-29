from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") >= 3.4) & F.col("city").isin(["Singapore", "Berlin", "Bangkok"]) & F.col("rating").between(2.0, 2.5))
    .groupBy("country", "city")
    .agg(
        F.count("*").alias("total_count"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
    )
)
