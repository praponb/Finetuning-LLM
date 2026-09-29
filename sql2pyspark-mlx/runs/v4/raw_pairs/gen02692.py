from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("cost", "id")
w2 = Window.partitionBy("carrier").orderBy("shipped_date", "id")

metrics = (
    df_shipments
    .select(
        "*",
        F.ntile(4).over(w).alias("cost_bucket"),
        F.lead("cost", 1).over(w2).alias("next_cost"),
    )
)

filtered = (
    metrics
    .filter(F.col("order_id").isNotNull())
    .select(
        "id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "next_cost",
        F.initcap(F.trim("carrier")).alias("carrier_title"),
    )
)

ranked = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

remaining_keys = (
    ranked
    .select("id", "carrier")
    .subtract(
        df_shipments
        .filter(F.col("delivered_date") > "2025-01-28")
        .select("id", "carrier")
    )
)

verified = (
    ranked
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.carrier") == F.col("k.carrier")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy("carrier")
    .agg(
        F.max("next_cost").alias("max_next_cost"),
        F.count(F.when(F.col("carrier") == "UPS", 1)).alias("ups_count"),
        F.percentile_approx("next_cost", [0.25, 0.5, 0.75]).alias("next_cost_quartiles"),
        F.round(F.sum("next_cost") / F.nullif(F.count(F.when(F.col("carrier") == "DHL", 1)), F.lit(0)), 2).alias("next_cost_per_dhl"),
        F.round(F.avg("next_cost"), 2).alias("avg_next_cost"),
    )
)
