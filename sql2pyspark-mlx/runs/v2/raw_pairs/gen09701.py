from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date") <= "2023-06-24")
    .groupBy(
        F.when(F.col("cost") >= 96, "high").when(F.col("cost") >= 18, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("carrier") == "Kerry", F.col("cost")).otherwise(0)).alias("kerry_cost"),
        F.concat_ws(", ", F.sort_array(F.collect_set("carrier"))).alias("carrier_values"),
    )
    .filter(F.col("kerry_cost") >= 63)
    .select("cost_bucket", "kerry_cost", "carrier_values")
    .orderBy(F.col("carrier_values").desc())
)
