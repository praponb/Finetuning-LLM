from pyspark.sql import functions as F

flattened_attrs = (
    df_carts
    .select(
        "id",
        "coupon_codes",
        "attributes",
        F.col("attributes")["weight_kg"].try_cast("double").alias("weight_kg"),
        F.forall("items", lambda item: item["qty"] <= 5).alias("all_small_qty"),
        F.array_max(F.transform("items", lambda item: item["price"])).alias("max_item_price"),
        F.col("attributes").getItem("utm_source").alias("utm_source"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        F.when(F.col("max_item_price") >= 208, "LARGE").otherwise("SMALL").alias("max_item_price_tier"),
        F.when(F.col("weight_kg") >= 3.0, "HIGH").otherwise("LOW").alias("weight_kg_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(F.col("weight_kg_tier").desc_nulls_first())
)
