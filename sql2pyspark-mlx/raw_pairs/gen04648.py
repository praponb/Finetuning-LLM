from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.when(F.col("sqft") >= 14828, "high").when(F.col("sqft") >= 6755, "mid").otherwise("low").alias("sqft_bucket"),
        F.quarter("opened_date").alias("opened_quarter"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("region"))).alias("region_values"),
        F.sum("sqft").alias("total_sqft"),
    )
    .filter(F.col("total_sqft") < 41000)
    .drop("total_sqft")
    .orderBy("sqft_bucket", "region_values")
)
