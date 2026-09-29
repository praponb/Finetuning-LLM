from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("is_jp").desc_nulls_last(), "id")

base = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        F.when(F.col("country") == "JP", 1).otherwise(0).alias("is_jp"),
    )
)

eligible = (
    base
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.product_name").like("La%")), "left_semi")
)

base_suppliers = (
    eligible
    .filter(~F.col("city").eqNullSafe(F.lit("Tokyo")))
    .select(
        "id",
        "supplier_name",
        "city",
        "is_jp",
        F.trim("country").alias("clean_country"),
        F.coalesce("city", F.lit("N/A")).alias("resolved_city"),
    )
)

deduped = (
    base_suppliers
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

kept = (
    deduped
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.supplier_id").isNotNull()), "left_anti")
)

result = (
    kept
    .groupBy(F.when(F.col("is_jp") >= 82, "HIGH").otherwise("LOW").alias("is_jp_tier"))
    .agg(
        F.sum(F.when(F.col("city") == "London", F.col("is_jp")).otherwise(0)).alias("london_is_jp"),
        F.size(F.collect_set("city")).alias("num_cities"),
        F.count(F.when(F.col("city") == "Singapore", 1)).alias("singapore_count"),
        F.max("is_jp").alias("max_is_jp"),
        F.count("*").alias("event_count"),
    )
)
