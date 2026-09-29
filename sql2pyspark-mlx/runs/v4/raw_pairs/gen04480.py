from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())

filtered = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "supplier_name",
        "city",
        F.substring("supplier_name", 1, 3).alias("supplier_name_prefix"),
        F.when(F.col("country").isin(["US", "UK"]), "Group A").otherwise("Other").alias("country_group"),
        F.coalesce("city", F.lit("OTHER")).alias("resolved_city"),
    )
)

latest_match = (
    filtered
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.cost"), F.col("c.brand").alias("product_brand"))
)

remaining_keys = (
    latest_match
    .select("id")
    .intersect(
        df_suppliers
        .filter(F.col("rating").isNotNull())
        .select("id")
    )
)

reconciled = (
    latest_match
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy("city")
    .agg(
        F.percentile_approx("cost", [0.25, 0.5, 0.75]).alias("cost_quartiles"),
        F.count(F.when(F.col("city") == "Bangkok", 1)).alias("bangkok_count"),
    )
)
