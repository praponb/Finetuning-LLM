from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy(F.col("rating").desc())
w2 = Window.partitionBy("city", "country").orderBy(F.col("rating").desc_nulls_first(), "id")

filtered = (
    df_suppliers
    .filter(F.col("country") == "US")
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.lower("country").alias("country_lower"),
    )
)

latest = (
    filtered
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

base = (
    latest
    .filter(F.col("city").isin(["Bangkok", "Tokyo", "Chiang Mai"]))
    .select(
        "*",
        F.col("dedup_rank").cast("string").alias("dedup_rank_num"),
        F.substring("supplier_name", 1, 2).alias("supplier_name_prefix"),
    )
)

ranked = (
    base
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

not_flagged = (
    ranked
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.array_contains(F.col("x.tags"), "clearance")), "left_semi")
)

result = (
    not_flagged
    .select(
        "id",
        "supplier_name",
        "country",
        "rating",
        "dedup_rank",
        "supplier_name_prefix",
        "latest_rank",
        F.when(F.col("country").rlike("^(US|JP)$"), "PRIORITY").otherwise("GROUP_B").alias("country_zone"),
    )
    .orderBy("id")
    .limit(5)
)
