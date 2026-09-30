from pyspark.sql import functions as F

src = df_suppliers.filter(F.lower(F.trim("supplier_name")) == "pacific wholesale")

rating_bucket_summary = (
    src
    .groupBy(
        F.when(F.col("rating") >= 4.2, "high").when(F.col("rating") >= 2.0, "mid").otherwise("low").alias("rating_bucket"),
        "country",
    )
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.concat_ws(", ", F.sort_array(F.collect_set("country"))).alias("country_values"),
    )
)

result = rating_bucket_summary
