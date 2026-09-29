from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("carrier") != "UPS") & F.col("carrier").isin(["UPS", "FedEx"]) & F.month("shipped_date").isin([3, 4, 7]))
    .groupBy("carrier")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "RETURNED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_returned"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        (F.max("cost") - F.min("cost")).alias("cost_range"),
    )
)
