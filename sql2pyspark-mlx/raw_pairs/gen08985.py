from pyspark.sql import functions as F

remaining_keys = (
    df_customers
    .select("city", "segment")
    .intersectAll(
        df_customers
        .filter(F.col("name").isNull())
        .select("city", "segment")
    )
)

reconciled = (
    df_customers
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.segment") == F.col("k.segment")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        "country",
        F.when(F.col("age") >= 67, "HIGH").otherwise("LOW").alias("age_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.size(F.collect_set("segment")).alias("num_segments"),
        F.max("credit_limit").alias("max_credit_limit"),
    )
    .filter(F.col("max_credit_limit") > 0)
)
