from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").desc(), "id")
w2 = Window.partitionBy("country").orderBy("rating", "id")
w3 = Window.partitionBy("country", "city").orderBy(F.col("rating").desc(), "id")
w4 = Window.partitionBy("city", "country").orderBy("rank_in_group", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "*",
        F.lower("city").alias("city_lower"),
        F.initcap(F.trim("city")).alias("city_title"),
    )
)

ranked = (
    base
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

ranked_suppliers = (
    ranked
    .withColumn("rank_in_group_2", F.row_number().over(w2))
    .filter(F.col("rank_in_group_2") == 1)
    .drop("rank_in_group_2")
)

eligible = (
    ranked_suppliers
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.supplier_id").isNotNull()), "left_anti")
)

eligible_suppliers = (
    eligible
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.year(F.col("ch.launch_date")) >= 2025), "left_anti")
)

enriched = (
    eligible_suppliers
    .filter(F.col("city") != "Chiang Mai")
    .select(
        "id",
        "supplier_name",
        "country",
        "city_title",
        "rank_in_group",
        F.ntile(4).over(w3).alias("rating_bucket"),
        F.sum("rating").over(w4).alias("cumulative_rating"),
    )
)

valid = (
    enriched
    .filter(F.col("rating_bucket").between(3, 8))
    .select(
        "id",
        "country",
        "city_title",
        "rank_in_group",
        "rating_bucket",
        "cumulative_rating",
        F.concat("country", F.lit(" - "), "supplier_name").alias("country_supplier_name"),
    )
)

result = (
    valid
    .groupBy(
        F.when(F.col("cumulative_rating") >= 19, "LARGE").otherwise("SMALL").alias("cumulative_rating_tier"),
    )
    .agg(
        F.round(F.avg("rating_bucket"), 2).alias("avg_rating_bucket"),
        F.count(F.when(F.col("country") == "US", 1)).alias("us_rows"),
        F.round(F.avg("rating_bucket"), 2).alias("avg_rating_bucket_2"),
    )
)
