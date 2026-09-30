from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")

remaining_keys = (
    df_suppliers
    .select("city")
    .intersect(
        df_suppliers
        .filter(F.col("country") != "JP")
        .select("city")
    )
)

verified = (
    df_suppliers
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

metrics = (
    verified
    .withColumn("rating_share", F.round((F.col("rating") / F.sum("rating").over(w)), 4))
    .withColumn("rating_share_2", F.round((F.col("rating") / F.sum("rating").over(w)), 4))
)

kept = (
    metrics
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.stock_qty") == 365), "left_anti")
)

eligible = (
    kept
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.cost") >= 480), "left_semi")
)

valid = (
    eligible
    .filter(F.col("city").isNotNull() & (~F.col("city").eqNullSafe(F.lit("Tokyo"))))
    .withColumn("rating_share_2_times_rating", (F.col("rating_share_2") * F.col("rating")))
    .withColumn("city_supplier_name", F.concat("city", F.lit(", "), "supplier_name"))
    .withColumn("city_clean", F.coalesce("city", F.lit("OTHER")))
)

result = (
    valid
    .groupBy("city")
    .pivot("country", ["US", "JP"])
    .agg(F.avg("rating_share_2").alias("value_sum"))
    .select("city", F.col("US").alias("us"), F.col("JP").alias("jp"))
)
