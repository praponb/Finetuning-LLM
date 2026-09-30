from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").desc(), "id")
w2 = Window.partitionBy("country").orderBy(F.col("rank_in_group").desc(), "id")

valid = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "*",
        F.concat("country", F.lit(", "), "supplier_name").alias("country_supplier_name"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("city_filled"),
    )
)

latest = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

ranked = (
    latest
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        "country_supplier_name",
        "rank_in_group",
    )
)

latest_suppliers = (
    ranked
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

kept = (
    latest_suppliers
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.supplier_id").isNull()), "left_semi")
)

result = (
    kept
    .groupBy(
        F.when(F.col("rating") >= 4.3, "LARGE").otherwise("SMALL").alias("rating_tier"),
    )
    .agg(
        F.count(F.when(F.col("city") == "Chiang Mai", 1)).alias("chiang_mai_count"),
        F.size(F.collect_set("city")).alias("num_cities"),
    )
    .orderBy(F.col("chiang_mai_count").desc_nulls_last())
)
