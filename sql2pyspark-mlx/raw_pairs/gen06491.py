from pyspark.sql import functions as F

cleaned = (
    df_shipments
    .filter(F.col("cost") <= 84)
    .select(
        "id",
        "order_id",
        "carrier",
        "delivered_date",
        "cost",
        "status",
        F.datediff(F.current_date(), "delivered_date").alias("days_since_delivered_date"),
        F.round((F.col("cost") * 1.07), 2).alias("adjusted_cost"),
        F.coalesce("carrier", F.lit("UNKNOWN")).alias("carrier_clean"),
    )
)

result = (
    cleaned
    .alias("a")
    .join(df_orders.alias("o"), F.col("a.order_id") == F.col("o.id"), "inner")
    .cube(F.col("o.channel").alias("channel"), F.col("a.carrier").alias("carrier"))
    .agg(
        F.count("*").alias("rows_total"),
        F.round(F.avg(F.col("a.adjusted_cost")), 2).alias("avg_adjusted_cost"),
        F.round(F.avg(F.col("a.cost")), 2).alias("avg_cost"),
        F.grouping_id().alias("gid"),
    )
)
