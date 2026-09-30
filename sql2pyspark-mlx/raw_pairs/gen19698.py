from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country").isin(["DE", "TH"]) & (F.col("city").isin(["Bangkok", "Berlin"]) | F.col("country").isin(["DE", "UK", "JP"])))
    .groupBy(
        F.when(F.col("rating") >= 3.9, "high").when(F.col("rating") >= 2.5, "mid").otherwise("low").alias("rating_bucket"),
        "country",
    )
    .agg(
        F.max("rating").alias("max_rating"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
    )
    .filter(F.col("max_rating") > 1.8)
)
