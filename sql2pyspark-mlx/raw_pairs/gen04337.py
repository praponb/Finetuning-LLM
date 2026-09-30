from pyspark.sql import functions as F

base = (
    df_inventory
    .filter(F.col("quantity") > 234)
    .select(
        "id",
        "quantity",
        F.ceil("quantity").alias("quantity_whole"),
        F.col("quantity").cast("double").alias("quantity_str"),
    )
)

base_inventory = (
    base
    .filter(F.col("quantity_whole") > 21)
    .select("*", F.ceil("quantity_str").alias("quantity_str_whole"))
)

valid = (
    base_inventory
    .filter(F.col("quantity").between(159, 199))
    .withColumn("quantity_str_plus_quantity", (F.col("quantity_str") + F.col("quantity")))
    .withColumn("quantity_str_whole_plus_quantity", (F.col("quantity_str_whole") + F.col("quantity")))
)

kept_keys = (
    valid
    .select("id")
    .subtract(
        df_inventory
        .filter(F.col("product_id").isin([6, 7, 8]))
        .select("id")
    )
)

kept_rows = (
    valid
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

summary = (
    kept_rows
    .groupBy(
        F.when(F.col("quantity_str_whole_plus_quantity") >= 69, "LARGE").otherwise("SMALL").alias("quantity_str_whole_plus_quantity_tier"),
        F.when(F.col("quantity_str") >= 63, "PREMIUM").otherwise("STANDARD").alias("quantity_str_tier"),
    )
    .agg(
        F.min("quantity").alias("min_quantity"),
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
    )
)

result = (
    summary
    .select(
        "quantity_str_whole_plus_quantity_tier",
        "quantity_str_tier",
        "min_quantity",
        "distinct_ids_2",
    )
    .orderBy(F.col("min_quantity").desc(), "quantity_str_tier")
)
