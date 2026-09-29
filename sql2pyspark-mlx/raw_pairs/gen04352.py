from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.year("delivered_date") == 2024) | (F.year("shipped_date") == 2024))
    .groupBy("status", "carrier")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "IN_TRANSIT", 1).otherwise(0))) / F.count("*")), 1).alias("pct_in_transit"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
    .filter(F.col("pct_in_transit") >= 56)
    .orderBy(F.col("status").desc(), F.col("pct_in_transit").desc())
)
