from pyspark.sql import functions as F

base = df_suppliers.filter(F.col("country").isin(["JP", "SG"]))

summary = (
    base
    .groupBy(
        F.when(F.col("rating") >= 3.6, "high").when(F.col("rating") >= 2.4, "mid").otherwise("low").alias("rating_bucket"),
        "country",
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("country"))).alias("country_values"),
    )
)

result = summary.orderBy(F.col("rating_bucket").desc(), "country")
