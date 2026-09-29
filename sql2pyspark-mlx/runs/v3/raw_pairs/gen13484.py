from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("carrier") == "FedEx") & (F.col("status") == "IN_TRANSIT"))
    .groupBy(
        F.when(F.col("cost") >= 84, "high").when(F.col("cost") >= 43, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("carrier"))).alias("carrier_values"),
    )
    .orderBy("carrier_values")
)
