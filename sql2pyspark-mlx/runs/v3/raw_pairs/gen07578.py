from pyspark.sql import functions as F

src = (
    df_suppliers
    .filter(F.col("rating").isNull())
    .withColumn("country_upper", F.upper("country"))
)

summary = (
    src
    .groupBy("city", "country")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("country"))).alias("country_values"),
        F.round(((100 * F.sum(F.when(F.col("city") == "Chiang Mai", 1).otherwise(0))) / F.count("*")), 1).alias("pct_chiang_mai"),
    )
)

result = summary.orderBy(F.col("pct_chiang_mai").desc())
