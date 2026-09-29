from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser", "device").orderBy("age", "id")
w2 = Window.orderBy(F.col("credit_limit").asc_nulls_last())
w3 = Window.partitionBy("country", "browser").orderBy(F.col("credit_limit").desc_nulls_first())
w_ordered = Window.partitionBy(F.to_date("started_at"), "device").orderBy(F.col("credit_limit").asc_nulls_last())

filtered = (
    df_sessions
    .filter((~F.col("country").eqNullSafe(F.lit("SG"))) & F.col("customer_id").isin([22, 32, 39]))
    .withColumn("browser_prefix", F.substring("browser", 1, 4))
    .withColumn("country_filled", F.coalesce("country", F.lit("DIRECT")))
)

filtered_sessions = (
    filtered
    .filter((F.col("browser").eqNullSafe(F.lit("Safari"))) & F.col("browser").isNotNull())
    .select(
        "*",
        F.date_trunc("year", "started_at").alias("started_at_period"),
        F.initcap(F.trim("browser")).alias("browser_title"),
        F.coalesce("browser", F.lit("NONE")).alias("browser_clean"),
    )
)

with_dims = (
    filtered_sessions
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "left")
    .select(
        "e.*",
        F.col("p.credit_limit"),
        F.col("p.country").alias("customer_country"),
        F.col("p.age"),
    )
)

ranked = (
    with_dims
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

remaining_keys = (
    ranked
    .select("customer_id")
    .subtract(
        df_sessions
        .filter(~F.col("browser").isin(["Safari", "Chrome"]))
        .select("customer_id")
    )
)

verified = (
    ranked
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .withColumn("credit_limit_dense_rank", F.dense_rank().over(w2))
    .withColumn("credit_limit_rank", F.rank().over(w3))
    .withColumn("credit_limit_percent_rank", F.percent_rank().over(w_ordered))
)

result = (
    enriched
    .groupBy("browser")
    .agg(
        F.count(F.when(F.col("browser") == "Edge", 1)).alias("edge_count"),
        F.round(F.avg("age"), 2).alias("avg_age"),
        F.max("started_at_period").alias("last_seen"),
        F.round(F.sum("credit_limit") / F.nullif(F.count(F.when(F.col("browser") == "Edge", 1)), F.lit(0)), 2).alias("credit_limit_per_edge"),
        F.count_if(F.col("country") == "TH").alias("th_events"),
        F.sort_array(F.collect_set("browser")).alias("browser_list"),
    )
    .filter(F.col("edge_count") > 1)
)
