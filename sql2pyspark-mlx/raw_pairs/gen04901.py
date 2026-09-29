from pyspark.sql import functions as F

flattened_attrs = (
    df_carts
    .select(
        "id",
        "ship_to",
        "attributes",
        F.col("attributes")["weight_kg"].try_cast("double").alias("weight_kg"),
        F.exists("items", lambda x: x["price"] > 100).alias("has_premium_item"),
        F.coalesce(F.col("attributes").getItem("device"), F.lit("none")).alias("device_or_default"),
        F.coalesce(F.col("attributes")["gift"], F.lit("direct")).alias("gift_or_default"),
        F.aggregate("items", F.lit(0.0), lambda acc, x: acc + x["qty"] * x["price"], lambda acc: F.round(acc, 2)).alias("basket_amount"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        F.when(F.col("basket_amount") >= 1800, "HIGH").otherwise("LOW").alias("basket_amount_tier"),
        F.when(F.col("weight_kg") >= 4.2, "HIGH").otherwise("LOW").alias("weight_kg_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("num_rows"),
        F.percentile_approx("weight_kg", [0.25, 0.5, 0.75]).alias("weight_kg_quartiles"),
        F.countDistinct("device_or_default").alias("distinct_device_or_defaults"),
    )
    .filter(F.col("num_rows") >= 1)
    .orderBy("distinct_device_or_defaults")
)
