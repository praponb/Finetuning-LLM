from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country")

kept_keys = (
    df_suppliers
    .select("id", "city")
    .exceptAll(
        df_suppliers
        .filter(F.length("supplier_name") < 5)
        .select("id", "city")
    )
)

reconciled = (
    df_suppliers
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

with_windows = reconciled.withColumn("city_max_rating", F.max("rating").over(w))

eligible = (
    with_windows
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.cost") <= 700), "left_anti")
)

not_flagged = (
    eligible
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (~F.col("ch.brand").isin(["Stark", "Globex"])), "left_anti")
)

not_flagged_suppliers = (
    not_flagged
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.category") == "Sports"), "left_anti")
)

daily = (
    not_flagged_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.1, "LARGE").otherwise("SMALL").alias("rating_tier"),
    )
    .agg(
        F.count(F.when(F.col("country") == "JP", 1)).alias("jp_count"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.avg("rating").alias("avg_rating"),
    )
)

result = (
    daily
    .select("rating_tier", "jp_count")
    .orderBy(F.col("jp_count").desc_nulls_first())
)
