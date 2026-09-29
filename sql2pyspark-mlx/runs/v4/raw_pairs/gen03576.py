from pyspark.sql import functions as F

flattened_attrs = (
    df_carts
    .select(
        "id",
        "items",
        "ship_to",
        "attributes",
        F.col("attributes")["weight_kg"].try_cast("double").alias("weight_kg"),
        F.forall("items", lambda item: item["qty"] <= 5).alias("all_small_qty"),
        F.coalesce(F.col("attributes")["device"], F.lit("direct")).alias("device_or_default"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        F.when(F.col("weight_kg") >= 4.0, "PREMIUM").otherwise("STANDARD").alias("weight_kg_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("weight_kg"), 2).alias("avg_weight_kg"),
        F.count("*").alias("num_rows"),
        F.avg("weight_kg").alias("avg_weight_kg_2"),
    )
    .orderBy(F.col("avg_weight_kg_2").asc_nulls_first())
)
