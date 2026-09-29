from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").desc())

base = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .withColumn("supplier_name_normalized", F.upper(F.trim("supplier_name")))
)

kept = (
    base
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.price") > 680), "left_anti")
)

kept_suppliers = (
    kept
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.array_contains(F.col("ch.tags"), "clearance")), "left_semi")
)

latest = (
    kept_suppliers
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "country", "rating")
)

matched_keys = (
    latest
    .select("country", "id")
    .intersectAll(
        df_suppliers
        .filter(F.col("rating") >= 1.9)
        .select("country", "id")
    )
)

verified = (
    latest
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.country") == F.col("k.country")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

kept_suppliers_2 = (
    verified
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.supplier_id") == 25), "left_anti")
)

result = kept_suppliers_2.select("id", "country", "rating")
