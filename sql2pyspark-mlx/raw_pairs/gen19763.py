from pyspark.sql import functions as F

parsed = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "items",
        "ship_to",
        F.col("ship_to.country").alias("ship_country"),
        F.col("attributes")["weight_kg"].try_cast("double").alias("weight_kg"),
    )
)

result = (
    parsed
    .groupBy(
        F.when(F.col("weight_kg") >= 3.8, "LARGE").otherwise("SMALL").alias("weight_kg_tier"),
        "ship_country",
    )
    .agg(
        F.percentile_approx("weight_kg", [0.25, 0.5, 0.75]).alias("weight_kg_quartiles"),
        F.round(F.avg("weight_kg"), 2).alias("avg_weight_kg"),
        F.count_if(F.col("ship_country") == "UK").alias("uk_events"),
    )
)
