from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy(F.col("days_since_created_at").desc(), "id", "attr_key")
w2 = Window.partitionBy("customer_id", "attr_key").orderBy("rank_in_group", "id", "attr_key").rowsBetween(-2, Window.currentRow)

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode("attributes").alias("attr_key", "value"),
    )
)

filtered = (
    flattened
    .filter(F.col("created_at") < "2023-07-11")
    .select(
        "id",
        "customer_id",
        "attr_key",
        "value",
        F.datediff(F.current_date(), "created_at").alias("days_since_created_at"),
        F.coalesce("attr_key", F.lit("NONE")).alias("attr_key_clean"),
    )
)

deduped = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

enriched = (
    deduped
    .select(
        "id",
        "attr_key",
        "value",
        "days_since_created_at",
        "rank_in_group",
        F.min("days_since_created_at").over(w2).alias("last3_min_days_since_created_at"),
        F.max("days_since_created_at").over(w2).alias("last3_max_days_since_created_at"),
        F.sum("days_since_created_at").over(w2).alias("last3_sum_days_since_created_at"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("last3_sum_days_since_created_at") >= 350, "PREMIUM").otherwise("STANDARD").alias("last3_sum_days_since_created_at_tier"),
        "attr_key",
    )
    .agg(
        F.count_if(F.col("attr_key") == "weight_kg").alias("weight_kg_events"),
        F.percentile_approx("days_since_created_at", [0.25, 0.5, 0.75]).alias("days_since_created_at_quartiles"),
        F.count_if(F.col("attr_key") == "utm_source").alias("utm_source_events"),
    )
    .orderBy(F.col("last3_sum_days_since_created_at_tier").desc_nulls_first())
)
