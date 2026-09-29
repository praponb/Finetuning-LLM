from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country").isin(["DE", "US"]) & (F.col("rating") <= 2.8))
    .agg(
        F.max("rating").alias("max_rating"),
        F.concat_ws(", ", F.sort_array(F.collect_set("country"))).alias("country_values"),
    )
    .orderBy("country_values", F.col("max_rating").desc())
)
