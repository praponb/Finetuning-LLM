from pyspark.sql import functions as F

remaining_keys = (
    df_shipments
    .select("id")
    .intersectAll(
        df_shipments
        .filter(F.col("carrier").isin(["UPS", "Flash", "DHL"]))
        .select("id")
    )
)

reconciled = (
    df_shipments
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(F.when(F.col("cost") >= 97, "LARGE").otherwise("SMALL").alias("cost_tier"))
    .agg(
        F.count(F.when(F.col("status") == "LOST", 1)).alias("lost_rows"),
        F.round(F.sum("cost") / F.nullif(F.count(F.when(F.col("status") == "LOST", 1)), F.lit(0)), 2).alias("cost_per_lost"),
        F.sort_array(F.collect_set("carrier")).alias("carrier_list"),
        F.percentile_approx("cost", [0.25, 0.5, 0.75]).alias("cost_quartiles"),
        F.sum(F.when(F.col("status") == "RETURNED", F.col("cost")).otherwise(0)).alias("returned_cost"),
        F.percentile_approx("cost", 0.95).alias("cost_p95"),
    )
    .filter(F.col("cost_per_lost") > 0)
    .orderBy(F.col("lost_rows").desc_nulls_last())
)
