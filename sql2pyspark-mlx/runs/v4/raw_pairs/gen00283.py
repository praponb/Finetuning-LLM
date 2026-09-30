from pyspark.sql import functions as F

filtered = (
    df_shipments
    .filter((F.col("shipped_date") > F.lit("2023-11-10").cast("date")) & F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.date_format("delivered_date", "yyyy-MM").alias("delivered_date_label"),
    )
)

result = (
    filtered
    .cube("status")
    .agg(
        F.count("*").alias("event_count"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.sum(F.when(F.col("status") == "IN_TRANSIT", F.col("cost")).otherwise(0)).alias("in_transit_cost"),
        F.count_if(F.col("status") == "RETURNED").alias("returned_events"),
        F.percentile_approx("cost", [0.25, 0.5, 0.75]).alias("cost_quartiles"),
        F.grouping_id().alias("grouping_id"),
    )
    .filter(F.col("event_count") < 1)
)
