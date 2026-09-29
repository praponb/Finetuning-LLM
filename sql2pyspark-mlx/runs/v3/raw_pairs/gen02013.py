from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isin(["Tokyo", "Singapore", "Bangkok"]))
    .groupBy("city")
    .agg(
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.sum(F.when(F.col("country") == "SG", 1).otherwise(0)).alias("sg_count"),
    )
    .filter(F.col("sg_count") >= 67)
    .orderBy("city")
    .limit(1)
)
