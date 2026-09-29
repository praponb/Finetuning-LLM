from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("flag_bit4").desc_nulls_last())
w2 = Window.orderBy(F.col("flag_bit4").asc_nulls_last())
w3 = Window.orderBy(F.col("status_code_high").desc_nulls_last())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

flattened_attrs = (
    exploded
    .select(
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("flag_bit4"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

enriched = (
    flattened_attrs
    .select(
        "flag_bit4",
        "status_code_high",
        F.cume_dist().over(w).alias("flag_bit4_cume_dist"),
    )
)

scored = (
    enriched
    .select(
        "status_code_high",
        "flag_bit4_cume_dist",
        F.rank().over(w2).alias("flag_bit4_rank"),
        F.cume_dist().over(w3).alias("status_code_high_cume_dist"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("flag_bit4_rank") >= 7, "HIGH").otherwise("LOW").alias("flag_bit4_rank_tier"),
    )
    .agg(
        F.sum("flag_bit4_rank").alias("total_flag_bit4_rank"),
        F.percentile_approx("flag_bit4_rank", [0.25, 0.5, 0.75]).alias("flag_bit4_rank_quartiles"),
        F.percentile_approx("status_code_high", [0.25, 0.5, 0.75]).alias("status_code_high_quartiles"),
        F.round(F.avg("flag_bit4_rank"), 2).alias("avg_flag_bit4_rank"),
        F.percentile_approx("status_code_high", 0.95).alias("status_code_high_p95"),
    )
    .orderBy("status_code_high_p95")
)
