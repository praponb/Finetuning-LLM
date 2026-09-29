from pyspark.sql import functions as F

lines = df_carts.select("id", "customer_id", F.explode("coupon_codes").alias("element"))

kept_keys = (
    lines
    .select("id")
    .intersectAll(
        df_carts
        .filter(F.col("created_at") >= "2024-10-01")
        .select("id")
    )
)

kept_rows = (
    lines
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

cleaned = (
    kept_rows
    .filter(F.col("element").isNotNull())
    .withColumn("element_group", F.when(F.col("element").isin(["WELCOME5", "SPRING10"]), "Core").otherwise("Other"))
)

enriched = (
    cleaned
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "left")
    .select("e.*", F.col("c.segment"))
)

result = (
    enriched
    .select("id", "customer_id", "element", "segment")
    .orderBy(F.col("element").desc_nulls_first())
)
