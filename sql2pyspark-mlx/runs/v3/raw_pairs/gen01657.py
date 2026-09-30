from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") > 9814)
    .cube(
        F.month("opened_date").alias("opened_month"),
        F.quarter("opened_date").alias("opened_quarter"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
        F.sum(F.when(F.col("city") == "Bangkok", 1).otherwise(0)).alias("bangkok_count"),
    )
)
