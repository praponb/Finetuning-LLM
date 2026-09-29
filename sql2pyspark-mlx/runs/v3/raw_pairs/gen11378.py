from pyspark.sql import functions as F

result = (
    df_stores
    .filter(~((F.col("city") == "Berlin") & ((F.col("region") != "East") | (F.col("region") == "Central"))))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("city") == "New York", 1).otherwise(0))) / F.count("*")), 1).alias("pct_new_york"),
        F.sort_array(F.collect_set("region")).alias("region_list"),
    )
)
