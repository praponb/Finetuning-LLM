from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.month("shipped_date").isin([4, 6, 10]))
    .rollup(
        F.when(F.col("carrier").isin(["Flash", "FedEx"]), "core").otherwise("other").alias("carrier_group"),
        F.when(F.col("cost") >= 79, "high").when(F.col("cost") >= 21, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.max("cost").alias("max_cost"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
)
