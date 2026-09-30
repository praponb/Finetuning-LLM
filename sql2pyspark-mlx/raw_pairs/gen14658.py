from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy("cost")
w_running = Window.partitionBy("carrier")

remaining_keys = (
    df_shipments
    .select("order_id")
    .intersect(
        df_shipments
        .filter(F.col("carrier").isin(["UPS", "FedEx", "Kerry"]))
        .select("order_id")
    )
)

kept_rows = (
    df_shipments
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .select(
        "id",
        "shipped_date",
        "delivered_date",
        F.rank().over(w_grp).alias("cost_rank"),
        F.min("cost").over(w_running).alias("carrier_min_cost"),
        F.min("cost").over(w_running).alias("carrier_min_cost_2"),
    )
)

base = (
    enriched
    .filter(F.col("delivered_date").between("2023-08-01", "2025-01-01"))
    .withColumn("cost_rank_num", F.col("cost_rank").cast("double"))
)

filtered = (
    base
    .filter(F.col("delivered_date") >= F.lit("2023-09-05").cast("date"))
    .select(
        "id",
        "shipped_date",
        "delivered_date",
        "cost_rank",
        "carrier_min_cost_2",
        "cost_rank_num",
        F.when(F.col("cost_rank") >= 9, "High").when(F.col("cost_rank") >= 4, "Medium").otherwise("Low").alias("cost_rank_band"),
        F.when(F.col("carrier_min_cost") >= 92, "High").when(F.col("carrier_min_cost") >= 24, "Medium").otherwise("Low").alias("carrier_min_cost_level"),
    )
)

valid = (
    filtered
    .filter(F.col("carrier_min_cost_2").isNotNull())
    .withColumn("cost_rank_num_int", F.col("cost_rank_num").cast("int"))
    .withColumn("shipped_date_weekofyear", F.weekofyear("shipped_date"))
)

result = (
    valid
    .groupBy(
        F.date_trunc("WEEK", "delivered_date").alias("week_start"),
        F.date_trunc("YEAR", "delivered_date").alias("year_start"),
    )
    .agg(
        F.avg("cost_rank_num_int").alias("avg_cost_rank_num_int"),
        F.round(F.avg("cost_rank_num"), 2).alias("avg_cost_rank_num"),
        F.round(F.avg("shipped_date_weekofyear"), 2).alias("avg_shipped_date_weekofyear"),
        F.sum("cost_rank").alias("total_cost_rank"),
        F.count("*").alias("num_rows"),
        F.count("*").alias("event_count"),
    )
)
